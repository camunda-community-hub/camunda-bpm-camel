/* Licensed under the Apache License, Version 2.0 (the "License");
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
package org.camunda.bpm.camel.component.producer;

import org.apache.camel.Exchange;
import org.camunda.bpm.camel.common.ExchangeUtils;
import org.camunda.bpm.camel.component.CamundaBpmEndpoint;
import org.camunda.bpm.engine.runtime.ProcessInstanceWithVariables;
import org.camunda.bpm.engine.runtime.ProcessInstantiationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.camunda.bpm.camel.component.CamundaBpmConstants.*;

/**
 * Starts a process instance given a process definition key.
 * <p>
 * Example: camunda-bpm://start?processDefinitionKey=aProcessDefinitionKey
 *
 * @author Ryan Johnston (@rjfsu)
 * @author Tijs Rademakers (@tijsrademakers)
 * @author Rafael Cordones (@rafacm)
 * @author Bernd Ruecker
 */
public class StartProcessProducer extends CamundaBpmProducer {

  private static final Logger LOG = LoggerFactory.getLogger(StartProcessProducer.class);

  private final String processDefinitionKey;

  public StartProcessProducer(CamundaBpmEndpoint endpoint, Map<String, Object> parameters) {
    super(endpoint, parameters);

    if (parameters.containsKey(PROCESS_DEFINITION_KEY_PARAMETER)) {
      this.processDefinitionKey = (String) parameters.get(PROCESS_DEFINITION_KEY_PARAMETER);
    } else {
        processDefinitionKey = null;
    }
  }

  @Override
  public void close() {
    LOG.info("Closing StartProcessProducer");
    this.stop();
  }

  @Override
  public void process(Exchange exchange) throws Exception {
    Map<String, Object> processVariables = new HashMap<>();
    if (parameters.containsKey(COPY_MESSAGE_PROPERTIES_PARAMETER)) {
      processVariables.putAll(exchange.getProperties());
    }
    if (parameters.containsKey(COPY_MESSAGE_HEADERS_PARAMETER)) {
      processVariables.putAll(exchange.getIn().getHeaders());
    }

    processVariables.putAll(ExchangeUtils.prepareVariables(exchange, parameters));

    /*
     * If the exchange contains the CAMUNDA_BPM_BUSINESS_KEY then we pass it to the engine
     */
    String processDefinitionKey = this.processDefinitionKey != null ? this.processDefinitionKey : exchange.getIn().getHeader(EXCHANGE_HEADER_PROCESS_DEFINITION_KEY, String.class);

    ProcessInstantiationBuilder procInstBuilder = runtimeService.createProcessInstanceByKey(processDefinitionKey);
    if (exchange.getProperties().containsKey(EXCHANGE_HEADER_BUSINESS_KEY)) {
      procInstBuilder.businessKey(exchange.getProperty(EXCHANGE_HEADER_BUSINESS_KEY, String.class));
    }
    ProcessInstanceWithVariables instance = procInstBuilder.setVariables(processVariables)
            .executeWithVariablesInReturn();

    exchange.setProperty(EXCHANGE_HEADER_BUSINESS_KEY, instance.getBusinessKey());

    exchange.setProperty(EXCHANGE_HEADER_PROCESS_DEFINITION_ID, instance.getProcessDefinitionId());
    exchange.setProperty(EXCHANGE_HEADER_PROCESS_INSTANCE_ID, instance.getProcessInstanceId());

    setOutBody(exchange, instance);
  }

  private void setOutBody(final Exchange exchange, final ProcessInstanceWithVariables instance) {
    if (parameters.containsKey(COPY_PROCESS_VARIABLES_TO_OUT_BODY_PARAMETER)) {
      final String variableName = parameters.get(COPY_PROCESS_VARIABLES_TO_OUT_BODY_PARAMETER).toString();

      if (variableName.equals("*")) {
        exchange.getMessage().setBody(instance.getVariables());
      } else if (variableName.contains(",")) {
        final HashMap<String, Object> variables = new HashMap<>();
        for (final String variableNameItem : variableName.split(",")) {
          Object val = instance.getVariables().get(variableNameItem);
          if (val != null) {
            variables.put(variableNameItem, val);
          }
        }
        exchange.getMessage().setBody(variables);
      } else if (!variableName.trim().isEmpty()) {
        exchange.getMessage().setBody(instance.getVariables().get(variableName));
      } else {
        exchange.getMessage().setBody(instance.getProcessInstanceId());
      }
    } else {
      exchange.getMessage().setBody(instance.getProcessInstanceId());
    }
  }
}