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

import static org.camunda.bpm.camel.component.CamundaBpmConstants.ASYNC_DEFAULT;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.ASYNC_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.LOCKDURATION_DEFAULT;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.LOCKDURATION_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.MAXTASKSPERPOLL_DEFAULT;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.MAXTASKSPERPOLL_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.RETRIES_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.RETRYTIMEOUTS_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.RETRYTIMEOUT_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.RETRYTIMEOUT_DEFAULT;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.TOPIC_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.WORKERID_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.VARIABLESTOFETCH_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.DESERIALIZEVARIABLES_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.DESERIALIZEVARIABLES_DEFAULT;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.camel.Consumer;
import org.apache.camel.PollingConsumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultPollingEndpoint;
import org.camunda.bpm.camel.component.externaltasks.BatchConsumer;
import org.camunda.bpm.camel.converter.TimePatternConverter;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.model.xml.impl.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CamundaBpmPollExternalTasksEndpointImpl extends DefaultPollingEndpoint implements CamundaBpmEndpoint {
    private static final Logger LOG = LoggerFactory.getLogger(
            CamundaBpmPollExternalTasksEndpointImpl.class.getCanonicalName());

    private final CamundaBpmComponent component;

    // parameters
    private final String topic;
    private final boolean completeTask;
    private final int retries;
    private final long retryTimeout;
    private final long[] retryTimeouts;
    private final int maxTasksPerPoll;
    private final long lockDuration;
    private final List<String> variablesToFetch;
    private final boolean deserializeVariables;
    private final String workerId;

    public CamundaBpmPollExternalTasksEndpointImpl(final String endpointUri, final CamundaBpmComponent component,
            final Map<String, Object> parameters) {
        super(endpointUri, component);
        this.component = component;
        if (parameters.containsKey(TOPIC_PARAMETER)) {
            this.topic = (String) parameters.remove(TOPIC_PARAMETER);
        } else {
            throw new IllegalArgumentException(
                    "You need to pass the '" + TOPIC_PARAMETER + "' parameter! Parameters received: " + parameters);
        }

        if (parameters.containsKey(RETRIES_PARAMETER)) {
            this.retries = Integer.parseInt((String) parameters.remove(RETRIES_PARAMETER));
        } else {
            this.retries = 0;
        }

        if (parameters.containsKey(RETRYTIMEOUT_PARAMETER)) {
            this.retryTimeout = TimePatternConverter.toMilliSeconds((String) parameters.remove(RETRYTIMEOUT_PARAMETER));
        } else {
            this.retryTimeout = RETRYTIMEOUT_DEFAULT;
        }

        if (parameters.containsKey(RETRYTIMEOUTS_PARAMETER)) {
            final String retryTimeoutsString = (String) parameters.remove(RETRYTIMEOUTS_PARAMETER);
            final String[] retryTimeoutsStrings = retryTimeoutsString.split(",");
            retryTimeouts = new long[retryTimeoutsStrings.length];
            for (int i = 0; i < retryTimeoutsStrings.length; ++i) {
                retryTimeouts[i] = TimePatternConverter.toMilliSeconds(retryTimeoutsStrings[i]);
            }
        } else {
            retryTimeouts = null;
        }

        if (parameters.containsKey(MAXTASKSPERPOLL_PARAMETER)) {
            this.maxTasksPerPoll = Integer.parseInt((String) parameters.remove(MAXTASKSPERPOLL_PARAMETER));
        } else {
            this.maxTasksPerPoll = MAXTASKSPERPOLL_DEFAULT;
        }

        if (parameters.containsKey(ASYNC_PARAMETER)) {
            this.completeTask = !Boolean.parseBoolean((String) parameters.remove(ASYNC_PARAMETER));
        } else {
            this.completeTask = !ASYNC_DEFAULT;
        }

        if (parameters.containsKey(LOCKDURATION_PARAMETER)) {
            this.lockDuration = TimePatternConverter.toMilliSeconds((String) parameters.remove(LOCKDURATION_PARAMETER));
        } else {
            this.lockDuration = LOCKDURATION_DEFAULT;
        }

        if (parameters.containsKey(WORKERID_PARAMETER)) {
            this.workerId = (String) parameters.remove(WORKERID_PARAMETER);
        } else {
            this.workerId = endpointUri;
        }

        if (parameters.containsKey(VARIABLESTOFETCH_PARAMETER)) {
            final String variables = (String) parameters.remove(VARIABLESTOFETCH_PARAMETER);
            if (variables.trim().isEmpty()) {
                variablesToFetch = new LinkedList<>();
            } else {
                variablesToFetch = StringUtil.splitListBySeparator(variables, ",");
            }
        } else {
            variablesToFetch = null;
        }

        if (parameters.containsKey(DESERIALIZEVARIABLES_PARAMETER)) {
            this.deserializeVariables = Boolean.parseBoolean(
                    (String) parameters.remove(DESERIALIZEVARIABLES_PARAMETER));
            if (!BatchConsumer.systemKnowsDeserializationOfVariables() && this.deserializeVariables) {
                LOG.warn("Parameter '" + DESERIALIZEVARIABLES_PARAMETER
                        + "' is set to 'true' but this setting will be ignored in this environment since the "
                        + " version of runtime Camunda is below 7.6.0!");
            }
        } else {
            this.deserializeVariables = DESERIALIZEVARIABLES_DEFAULT;
        }
    }

    @Override
    public void close() {
        LOG.info("Closing CamundaBpmPollExternalTasksEndpointImpl");
        super.stop();
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        final BatchConsumer consumer;
        if (getScheduledExecutorService() != null) {
            consumer = new BatchConsumer(this,
                    processor,
                    getScheduledExecutorService(),
                    retries,
                    retryTimeout,
                    retryTimeouts,
                    lockDuration,
                    topic,
                    completeTask,
                    variablesToFetch,
                    deserializeVariables,
                    workerId);
        } else {
            consumer = new BatchConsumer(this,
                    processor,
                    retries,
                    retryTimeout,
                    retryTimeouts,
                    lockDuration,
                    topic,
                    completeTask,
                    variablesToFetch,
                    deserializeVariables,
                    workerId);
        }
        configureConsumer(consumer);
        consumer.setMaxMessagesPerPoll(maxTasksPerPoll);

        return consumer;
    }

    @Override
    public PollingConsumer createPollingConsumer() {
        return null;
    }

    @Override
    public Producer createProducer() {
        return null;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public ProcessEngine getProcessEngine() {
        return component.getProcessEngine();
    }
}
