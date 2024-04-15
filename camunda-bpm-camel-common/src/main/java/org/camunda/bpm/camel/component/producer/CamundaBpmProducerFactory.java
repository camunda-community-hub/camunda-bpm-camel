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

import java.util.Map;

import org.camunda.bpm.camel.common.UriUtils.ParsedUri;
import org.camunda.bpm.camel.component.CamundaBpmEndpoint;

/**
 * Creates producers according to the URI passed
 */
public final class CamundaBpmProducerFactory {

    private CamundaBpmProducerFactory() {
        throw new UnsupportedOperationException("This class cannot be instantiated");
    }

    public static CamundaBpmProducer createProducer(final CamundaBpmEndpoint endpoint, final ParsedUri uri,
            final Map<String, Object> parameters) throws IllegalArgumentException {
        return switch (uri.getType()) {
            case START_PR -> new StartProcessProducer(endpoint, parameters);
            case SEND_SIGNAL, SEND_MESSAGE -> new MessageProducer(endpoint, parameters);
            default -> throw new IllegalArgumentException("Cannot create a producer for URI '" + uri
                + "' - new ProducerType '" + uri.getType() + "' not yet supported?");
        };

    }

}
