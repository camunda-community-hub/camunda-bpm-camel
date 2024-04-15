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
package org.camunda.bpm.camel.common;

import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_BUSINESS_KEY;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_CORRELATION_KEY;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.support.DefaultExchange;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.pvm.delegate.ActivityExecution;
import org.camunda.bpm.engine.delegate.BpmnError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CamelServiceCommonImpl implements CamelService {

  final Logger log = LoggerFactory.getLogger(this.getClass());

  protected ProcessEngine processEngine;
  protected CamelContext camelContext;

  @Override
  public Object sendTo(String endpointUri) throws Exception {
    return sendTo(endpointUri, null, null);
  }

  @Override
  public Object sendTo(String endpointUri, String processVariables) throws Exception{
    return sendTo(endpointUri, processVariables, null);
  }

  @Override
  public Object sendTo(String endpointUri, String processVariables,
      String correlationId) throws Exception {
    Collection<String> vars;
    if (processVariables == null) {
      vars = new LinkedList<>();
      ActivityExecution execution = Context.getBpmnExecutionContext()
          .getExecution();
      final Set<String> variableNames = execution.getVariableNames();
      if (variableNames != null) {
        for (String variableName : variableNames) {
          vars.add(variableName + "?");
        }
      }
    } else if (processVariables.isEmpty()) {
      vars = Collections.emptyList();
    } else {
      vars = Arrays.asList(processVariables.split("\\s*,\\s*"));
    }
    return sendToInternal(endpointUri, vars, correlationId);
  }

  private Object sendToInternal(
      String endpointUri,
      Collection<String> variables, String correlationKey) throws Exception {
    ActivityExecution execution = Context.getBpmnExecutionContext().getExecution();
    Map<String, Object> variablesToSend = new HashMap<>();
    for (String variable : variables) {
      Object value;
      if (variable.endsWith("?")) {
        value = execution.getVariable(variable.substring(0, variable.length() - 1));
      } else {
        value = execution.getVariable(variable);
        if (value == null) {
          throw new IllegalArgumentException("Process variable '" + variable + "' no found!");
        }
      }
      variablesToSend.put(variable, value);
    }

    log.debug(
        "Sending process variables '{}' as a map to Camel endpoint '{}'",
        variablesToSend,
        endpointUri);
    try (ProducerTemplate producerTemplate = camelContext.createProducerTemplate()) {
      String businessKey = execution.getBusinessKey();

      Exchange exchange = new DefaultExchange(camelContext);
      exchange.setProperty(EXCHANGE_HEADER_PROCESS_INSTANCE_ID, execution.getProcessInstanceId());
      if (businessKey != null) {
        exchange.setProperty(EXCHANGE_HEADER_BUSINESS_KEY, businessKey);
      }
      if (correlationKey != null) {
        exchange.setProperty(EXCHANGE_HEADER_CORRELATION_KEY, correlationKey);
      }
      exchange.getIn().setBody(variablesToSend);
      exchange.setPattern(ExchangePattern.InOut);
      Exchange send = producerTemplate.send(endpointUri, exchange);

      // Exception handling
      //    Propagate BpmnError back from camel route,
      //    all other exceptions will cause workflow to stop as a technical error
      // https://docs.camunda.org/get-started/rpa/error-handling/
      // https://docs.camunda.org/manual/7.15/reference/bpmn20/events/error-events/
      if (null != send.getException()) {
        // Explicit BPMN business error, workflow has a chance to handle on boundary event, throw as is
        // Note that this can terminate a process instance if no handling is defined in the model (https://docs.camunda.org/manual/latest/user-guide/process-engine/delegation-code/#throw-bpmn-errors-from-listeners)
        if (send.getException() instanceof BpmnError) {
          throw send.getException();
        }

        // otherwise simply throw the exception, leads to incident in process instance
        throw send.getException();
      }

      return send.getIn().getBody();
    }
  }

  public abstract void setProcessEngine(ProcessEngine processEngine);

  public abstract void setCamelContext(CamelContext camelContext);
}
