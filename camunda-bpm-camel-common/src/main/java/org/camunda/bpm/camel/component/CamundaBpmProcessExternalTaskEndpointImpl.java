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

import static org.camunda.bpm.camel.component.CamundaBpmConstants.ONCOMPLETION_DEFAULT;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.ONCOMPLETION_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.RETRIES_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.RETRYTIMEOUTS_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.RETRYTIMEOUT_DEFAULT;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.RETRYTIMEOUT_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.TOPIC_PARAMETER;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.WORKERID_PARAMETER;

import java.util.Map;
import org.apache.camel.Processor;
import org.apache.camel.support.ProcessorEndpoint;
import org.camunda.bpm.camel.component.externaltasks.TaskProcessor;
import org.camunda.bpm.camel.converter.TimePatternConverter;
import org.camunda.bpm.engine.ProcessEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CamundaBpmProcessExternalTaskEndpointImpl extends ProcessorEndpoint
    implements CamundaBpmEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(
      CamundaBpmProcessExternalTaskEndpointImpl.class);

  private final CamundaBpmComponent component;

  // parameters
  private final String topic;
  private final boolean onCompletion;
  private final int retries;
  private final long retryTimeout;
  private final long[] retryTimeouts;
  private final String workerId;

  public CamundaBpmProcessExternalTaskEndpointImpl(final String endpointUri,
      final CamundaBpmComponent component,
      final Map<String, Object> parameters) {
    super(endpointUri, component);
    this.component = component;

    if (parameters.containsKey(TOPIC_PARAMETER)) {
      this.topic = (String) parameters.remove(TOPIC_PARAMETER);
    } else {
      this.topic = null;
    }

    if (parameters.containsKey(RETRIES_PARAMETER)) {
      this.retries = Integer.parseInt((String) parameters.remove(RETRIES_PARAMETER));
    } else {
      this.retries = 0;
    }

    if (parameters.containsKey(RETRYTIMEOUT_PARAMETER)) {
      this.retryTimeout = TimePatternConverter.toMilliSeconds(
          (String) parameters.remove(RETRYTIMEOUT_PARAMETER));
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

    if (parameters.containsKey(ONCOMPLETION_PARAMETER)) {
      this.onCompletion = Boolean.parseBoolean((String) parameters.remove(ONCOMPLETION_PARAMETER));
    } else {
      this.onCompletion = ONCOMPLETION_DEFAULT;
    }

    if (parameters.containsKey(WORKERID_PARAMETER)) {
      this.workerId = (String) parameters.remove(WORKERID_PARAMETER);
    } else {
      this.workerId = null;
    }
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

  @Override
  public void close() {
    LOG.info("Closing CamundaBpmProcessExternalTaskEndpointImpl");
    this.stop();
  }

  @Override
  public ProcessEngine getProcessEngine() {
    return component.getProcessEngine();
  }

  @Override
  protected Processor createProcessor() {
    return new TaskProcessor(
        this,
        topic,
        retries,
        retryTimeout,
        retryTimeouts,
        true,
        onCompletion,
        workerId);
  }

}
