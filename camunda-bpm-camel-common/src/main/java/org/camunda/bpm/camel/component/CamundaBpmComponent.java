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

import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.support.DefaultComponent;
import org.camunda.bpm.camel.common.UriUtils.ParsedUri;
import org.camunda.bpm.engine.ProcessEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * camunda BPM Apache Camel component
 * 
 * @author Ryan Johnston (@rjfsu)
 * @author Tijs Rademakers (@tijsrademakers)
 * @author Rafael Cordones (@rafacm)
 */
public class CamundaBpmComponent extends DefaultComponent {
    final Logger log = LoggerFactory.getLogger(CamundaBpmComponent.class);

    protected ProcessEngine processEngine;

    public CamundaBpmComponent() {
    }

    public CamundaBpmComponent(ProcessEngine processEngine) {
        super();
        this.processEngine = processEngine;
    }

    @Override
    public void close() {
        log.info("Closing CamundaBpmComponent");
        super.stop();
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
        final ParsedUri parsedUri = new ParsedUri(remaining);
      return switch (parsedUri.getType()) {
        case POLL_EXTERNAL_TASKS ->
            new CamundaBpmPollExternalTasksEndpointImpl(uri, this, parameters);
        case PROCESS_EXTERNAL_TASK ->
            new CamundaBpmProcessExternalTaskEndpointImpl(uri, this, parameters);
        default -> new CamundaBpmEndpointDefaultImpl(uri, parsedUri, this, parameters);
      };
    }

    public ProcessEngine getProcessEngine() {
        return this.processEngine;
    }

    public void setProcessEngine(ProcessEngine processEngine) {
        this.processEngine = processEngine;
    }
}
