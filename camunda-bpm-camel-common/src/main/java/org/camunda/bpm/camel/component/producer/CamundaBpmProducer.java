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

import org.apache.camel.support.DefaultProducer;
import org.camunda.bpm.camel.component.CamundaBpmEndpoint;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.RuntimeService;

import java.util.Map;

public abstract class CamundaBpmProducer extends DefaultProducer {

  protected ProcessEngine processEngine;
  protected RuntimeService runtimeService;
  protected Map<String, Object> parameters;

  protected CamundaBpmProducer(CamundaBpmEndpoint endpoint, Map<String, Object> parameters) {
    super(endpoint);
    this.processEngine = endpoint.getProcessEngine();
      if (this.processEngine != null) {
          this.runtimeService = processEngine.getRuntimeService();
      }
    this.parameters = parameters;
  }

  protected CamundaBpmEndpoint getCamundaBpmEndpoint() {
    return (CamundaBpmEndpoint) getEndpoint();
  }  
}
