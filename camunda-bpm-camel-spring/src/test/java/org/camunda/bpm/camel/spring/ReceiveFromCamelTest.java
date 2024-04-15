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
import org.apache.camel.ProducerTemplate;
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
@ContextConfiguration("classpath:receive-from-camel-config.xml")
public class ReceiveFromCamelTest {
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
  @Deployment(resources = {"process/ReceiveFromCamel.bpmn20.xml"})
  public void doTest() {
    Map<String, Object> processVariables = new HashMap<>();
    processVariables.put("var1", "foo");
    processVariables.put("var2", "bar");
    ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("receiveFromCamelProcess", processVariables);

    // Verify that a process instance has executed and there is one instance executing now
    Assertions
        .assertThat(historyService
            .createHistoricProcessInstanceQuery()
            .processDefinitionKey("receiveFromCamelProcess")
            .count())
        .isEqualTo(1);
    Assertions
        .assertThat(runtimeService
            .createProcessInstanceQuery()
            .processDefinitionKey("receiveFromCamelProcess")
            .count())
        .isEqualTo(1);

    /*
     * We need the process instance ID to be able to send the message to it
     *
     * FIXE: we need to fix this with the process execution id or even better with the Activity Instance Model
     * http://camundabpm.blogspot.de/2013/06/introducing-activity-instance-model-to.html
     */
    ProducerTemplate tpl = camelContext.createProducerTemplate();
    tpl.sendBodyAndProperty(
        "direct:sendToCamundaBpm", 
        null, 
        CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID, 
        processInstance.getId());

    // Assert that the camunda BPM process instance ID has been added as a property to the message
    Assertions
        .assertThat(mockEndpoint
            .assertExchangeReceived(0)
            .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID))
        .isEqualTo(processInstance.getId());

    // Assert that the process instance is finished
    Assertions
        .assertThat(runtimeService
            .createProcessInstanceQuery()
            .processDefinitionKey("receiveFromCamelProcess")
            .count())
        .isZero();
  }
}
