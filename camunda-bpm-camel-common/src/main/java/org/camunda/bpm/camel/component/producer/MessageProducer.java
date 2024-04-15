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

import static org.camunda.bpm.camel.component.CamundaBpmConstants.ACTIVITY_ID_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_BUSINESS_KEY;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_CORRELATION_KEY;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_CORRELATION_KEY_TYPE;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.CORRELATION_KEY_NAME_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.MESSAGE_NAME_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.PROCESS_DEFINITION_KEY_PARAMETER;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.camunda.bpm.camel.common.ExchangeUtils;
import org.camunda.bpm.camel.component.CamundaBpmEndpoint;
import org.camunda.bpm.engine.runtime.Execution;
import org.camunda.bpm.engine.runtime.ExecutionQuery;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.runtime.ProcessInstanceQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends a message (or signals a ReceiveTask) to a waiting process instance or
 * start a new process instance by message.
 * 
 * Example: camunda-bpm://message?messageName=someMessage
 * 
 * Example: camunda-bpm://message?activityId=receiveTask
 * 
 * @author Rafael Cordones (@rafacm)
 * @author Bernd Ruecker
 */
public class MessageProducer extends CamundaBpmProducer {
    private static final Logger LOG = LoggerFactory.getLogger(MessageProducer.class);

    private final String messageName;
    private final String activityId;
    private final String processDefinitionKey;
    private final String correlationKeyName;

    public MessageProducer(CamundaBpmEndpoint endpoint, Map<String, Object> parameters) {
        super(endpoint, parameters);

        if (parameters.containsKey(MESSAGE_NAME_PARAMETER)) {
            this.messageName = (String) parameters.get(MESSAGE_NAME_PARAMETER);
            this.activityId = null;
        } else if (parameters.containsKey(ACTIVITY_ID_PARAMETER)) {
            this.messageName = null;
            this.activityId = (String) parameters.get(ACTIVITY_ID_PARAMETER);
        } else {
            this.messageName = null;
            this.activityId = null;
            throw new IllegalArgumentException("You need to pass the '" + MESSAGE_NAME_PARAMETER
                    + "' parameter! Parameters received: " + parameters);
        }
        if (parameters.containsKey(PROCESS_DEFINITION_KEY_PARAMETER)) {
            this.processDefinitionKey = (String) parameters.get(PROCESS_DEFINITION_KEY_PARAMETER);
        } else {
            this.processDefinitionKey = null;
        }
        if (parameters.containsKey(CORRELATION_KEY_NAME_PARAMETER)) {
            this.correlationKeyName = (String) parameters.get(CORRELATION_KEY_NAME_PARAMETER);
        } else {
            this.correlationKeyName = null;
        }
    }

    @Override
    public void close() {
        LOG.info("Closing MessageProducer");
        this.stop();
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String processInstanceId = exchange.getProperty(EXCHANGE_HEADER_PROCESS_INSTANCE_ID, String.class);
        String businessKey = exchange.getProperty(EXCHANGE_HEADER_BUSINESS_KEY, String.class);

        Map<String, Object> processVariables = ExchangeUtils.prepareVariables(exchange, parameters);

        if (messageName != null) {
            HashMap<String, Object> correlationKeys = new HashMap<>();

            if (correlationKeyName != null) {
                Class<?> clazz = String.class;
                String correlationKeyType = exchange.getProperty(EXCHANGE_HEADER_CORRELATION_KEY_TYPE, String.class);
                if (correlationKeyType != null) {
                    clazz = Class.forName(correlationKeyType);
                }
                Object correlationKey = exchange.getProperty(EXCHANGE_HEADER_CORRELATION_KEY, clazz);
                if (correlationKey == null) {
                    throw new RuntimeException("Missing value for correlation key for message '" + messageName + "'");
                }
                correlationKeys.put(correlationKeyName, correlationKey);
            }

            // if we have process instance we try to send the message to this
            // one:
            if (processInstanceId != null) {
                ExecutionQuery query = runtimeService.createExecutionQuery() //
                        .processInstanceId(processInstanceId) //
                        .messageEventSubscriptionName(messageName);
                if (processDefinitionKey != null) {
                    query.processDefinitionKey(processDefinitionKey);
                }
                Execution execution = query.singleResult();

                if (execution == null) {
                    throw new RuntimeException("Couldn't find waiting process instance with id '" + processInstanceId
                            + "' for message '" + messageName + "'");
                }

                runtimeService.messageEventReceived(messageName, execution.getId(), processVariables);
            } else if (businessKey != null) {
                // if we have businessKey, use it to correlate
                runtimeService.correlateMessage(messageName, businessKey, correlationKeys, processVariables);
            } else {
                // otherwise we just send the message to the engine to let the
                // engine
                // decide what to do
                // this can either correlate to a waiting instance or start a
                // new
                // process Instance

                runtimeService.correlateMessage(messageName, correlationKeys, processVariables);
            }
        } else {
            // signal a ReceiveTask needs a processInstance to be addressed
            // (hint: this should be best done by a message in the ReceiveTask
            // as this is possible from 7.1 on - this was introduced with 7.0
            // where this was not yet possible)
            if (processInstanceId == null && businessKey != null) {
                ProcessInstanceQuery query = runtimeService.createProcessInstanceQuery().processInstanceBusinessKey(
                        businessKey);
                if (processDefinitionKey != null) {
                    query.processDefinitionKey(processDefinitionKey);
                }
                ProcessInstance processInstance = query.singleResult();
                if (processInstance != null) {
                    processInstanceId = processInstance.getId();
                }
            }

            if (processInstanceId == null) {
                throw new RuntimeException("Could not find the process instance via the provided properties ("
                        + EXCHANGE_HEADER_PROCESS_INSTANCE_ID + "= '" + processInstanceId + "', " + EXCHANGE_HEADER_BUSINESS_KEY
                        + "= '" + businessKey + "'");
            }

            ExecutionQuery query = runtimeService.createExecutionQuery().processInstanceId(
                    processInstanceId).activityId(activityId);

            if (processDefinitionKey != null) {
                query.processDefinitionKey(processDefinitionKey);
            }
            Execution execution = query.singleResult();

            if (execution == null) {
                throw new RuntimeException("Couldn't find process instance with id '" + processInstanceId
                        + "' waiting in activity '" + activityId + "'");
            }

            runtimeService.signal(execution.getId(), processVariables);
        }
    }
}
