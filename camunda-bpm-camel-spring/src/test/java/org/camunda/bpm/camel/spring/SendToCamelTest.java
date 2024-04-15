package org.camunda.bpm.camel.spring;/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.camel.CamelContext;
import org.apache.camel.component.mock.MockEndpoint;
import org.assertj.core.api.Assertions;
import org.camunda.bpm.camel.component.CamundaBpmConstants;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.test.Deployment;
import org.camunda.bpm.engine.test.ProcessEngineRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:send-to-camel-config.xml")
public class SendToCamelTest {

  MockEndpoint mockEndpoint;

  @Autowired
  ApplicationContext applicationContext;

  @Autowired
  CamelContext camelContext;

  @Autowired
  RuntimeService runtimeService;

  @Autowired
  HistoryService historyService;

  @Autowired
  @Rule
  public ProcessEngineRule processEngineRule;

  @Before
  public void setUp() {
    mockEndpoint = (MockEndpoint) camelContext.getEndpoint("mock:endpoint");
    mockEndpoint.reset();
  }

  @Test
  @Deployment(resources = {"process/SendToCamel.bpmn20.xml"})
  public void doTest() {
    Map<String, Object> processVariables = new HashMap<>();
    processVariables.put("var1", "foo");
    processVariables.put("var2", "bar");
    ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("sendToCamelProcess", processVariables);

    // Verify that a process instance was executed and there are no instances executing now
    Assertions
        .assertThat(historyService
            .createHistoricProcessInstanceQuery()
            .processDefinitionKey("sendToCamelProcess")
            .count())
        .isEqualTo(1);
    Assertions
        .assertThat(runtimeService
            .createProcessInstanceQuery()
            .processDefinitionKey("sendToCamelProcess")
            .count())
        .isZero();

    // Assert that the camunda BPM process instance ID has been added as a property to the message
    Assertions.assertThat(mockEndpoint
        .assertExchangeReceived(0)
        .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID))
        .isEqualTo(processInstance.getId());

    // Assert that the body of the message received by the endpoint contains a hash map with the value of the process variable 'var1' sent from camunda BPM
    Assertions
        .assertThat(mockEndpoint
            .assertExchangeReceived(0)
            .getIn()
            .getBody(String.class))
        .isEqualTo("{var1=foo}");

    // FIXME: check that var2 is also present as a property!
  }
}
