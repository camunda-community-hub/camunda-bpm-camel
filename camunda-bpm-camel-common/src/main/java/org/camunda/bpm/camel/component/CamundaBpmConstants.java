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
package org.camunda.bpm.camel.component;

/**
 * Common constants for the camunda BPM Apache Camel component
 */
public final class CamundaBpmConstants {

    public static final String CAMUNDA_BPM_CAMEL_URI_SCHEME = "camunda-bpm";

    public static final String EXCHANGE_HEADER_PROCESS_DEFINITION_KEY = "CamundaBpmProcessDefinitionKey";
    public static final String EXCHANGE_HEADER_PROCESS_DEFINITION_ID = "CamundaBpmProcessDefinitionId";
    public static final String EXCHANGE_HEADER_PROCESS_INSTANCE_ID = "CamundaBpmProcessInstanceId";
    public static final String EXCHANGE_HEADER_PROCESS_PRIO = "CamundaBpmProcessInstancePrio";
    public static final String EXCHANGE_HEADER_BUSINESS_KEY = "CamundaBpmBusinessKey";
    public static final String EXCHANGE_HEADER_CORRELATION_KEY = "CamundaBpmCorrelationKey";
    public static final String EXCHANGE_HEADER_CORRELATION_KEY_TYPE = "CamundaBpmCorrelationKeyType";
    public static final String EXCHANGE_HEADER_TASK = "CamundaBpmExternalTask";
    public static final String EXCHANGE_HEADER_RETRIESLEFT = "CamundaBpmExternalRetriesLeft";
    public static final String EXCHANGE_HEADER_ATTEMPTSSTARTED = "CamundaBpmExternalAttemptsStarted";
    public static final String EXCHANGE_HEADER_TASKID = "CamundaBpmExternalTaskId";
    public static final String EXCHANGE_RESPONSE_IGNORE = "CamundaBpmExternalTaskIgnore";

    /* Apache Camel URI parameters */
    public static final String PROCESS_DEFINITION_KEY_PARAMETER = "processDefinitionKey";
    public static final String TOPIC_PARAMETER = "topic";
    public static final String WORKERID_PARAMETER = "workerId";
    public static final String VARIABLESTOFETCH_PARAMETER = "variablesToFetch";
    public static final String DESERIALIZEVARIABLES_PARAMETER = "deserializeVariables";
    public static final boolean DESERIALIZEVARIABLES_DEFAULT = true;
    public static final String MAXTASKSPERPOLL_PARAMETER = "maxTasksPerPoll";
    public static final int MAXTASKSPERPOLL_DEFAULT = 5;
    public static final String ASYNC_PARAMETER = "async";
    public static final boolean ASYNC_DEFAULT = false;
    public static final String ONCOMPLETION_PARAMETER = "onCompletion";
    public static final boolean ONCOMPLETION_DEFAULT = false;
    public static final String LOCKDURATION_PARAMETER = "lockDuration";
    public static final long LOCKDURATION_DEFAULT = 60000;
    public static final String RETRIES_PARAMETER = "retries";
    public static final String RETRYTIMEOUT_PARAMETER = "retryTimeout";
    public static final long RETRYTIMEOUT_DEFAULT = 500;
    public static final String RETRYTIMEOUTS_PARAMETER = "retryTimeouts";
    public static final String MESSAGE_NAME_PARAMETER = "messageName";
    public static final String CORRELATION_KEY_NAME_PARAMETER = "correlationKeyName";
    public static final String ACTIVITY_ID_PARAMETER = "activityId";
    public static final String COPY_MESSAGE_PROPERTIES_PARAMETER = "copyProperties";
    public static final String COPY_MESSAGE_HEADERS_PARAMETER = "copyHeaders";
    public static final String COPY_MESSAGE_BODY_AS_PROCESS_VARIABLE_PARAMETER = "copyBodyAsVariable";
    public static final String COPY_PROCESS_VARIABLES_TO_OUT_BODY_PARAMETER = "copyVariablesToOutBody";

    private CamundaBpmConstants() {
        throw new UnsupportedOperationException("This class cannot be instantiated");
    }

    public static String camundaBpmUri(String path) {
        return CAMUNDA_BPM_CAMEL_URI_SCHEME + ":" + path;
    }
}
