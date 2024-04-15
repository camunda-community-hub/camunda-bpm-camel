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

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultEndpoint;
import org.camunda.bpm.camel.common.UriUtils.ParsedUri;
import org.camunda.bpm.camel.component.producer.CamundaBpmProducerFactory;
import org.camunda.bpm.engine.ProcessEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has been modified to be consistent with the changes to
 * CamelBehavior and its implementations. The set of changes significantly
 * increases the flexibility of our Camel integration, as you can either choose
 * one of three "out-of-the-box" modes, or you can choose to create your own.
 * Please reference the comments for the "CamelBehavior" class for more
 * information on the out-of-the-box implementation class options.
 * 
 * @author Ryan Johnston (@rjfsu), Tijs Rademakers
 */
public class CamundaBpmEndpointDefaultImpl extends DefaultEndpoint implements CamundaBpmEndpoint {
    private static final Logger LOG = LoggerFactory.getLogger(CamundaBpmEndpointDefaultImpl.class);

    private final CamundaBpmComponent component;
    private final Map<String, Object> parameters;
    private final ParsedUri uri;

    public CamundaBpmEndpointDefaultImpl(String uri, ParsedUri parsedUri, CamundaBpmComponent component,
            Map<String, Object> parameters) {
        super(uri, component);
        this.uri = parsedUri;
        this.component = component;
        this.parameters = parameters;
    }

    public ProcessEngine getProcessEngine() {
        return this.component.getProcessEngine();
    }

    public Producer createProducer() {
        return CamundaBpmProducerFactory.createProducer(this, this.uri, this.parameters);
    }

    public Consumer createConsumer(Processor processor) {
        return null;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void close() {
        LOG.info("Closing CamundaBpmEndpointDefaultImpl");
        super.stop();
    }

    @Override
    public boolean isLenientProperties() {
        return true;
    }
}
