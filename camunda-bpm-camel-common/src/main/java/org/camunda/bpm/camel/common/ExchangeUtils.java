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

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.camunda.bpm.camel.component.CamundaBpmConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains one method - prepareVariables - that is used to copy
 * variables from Camel into camunda BPM.
 * 
 * @author Ryan Johnston (@rjfsu), Tijs Rademakers
 * @author Bernd Ruecker
 */
public class ExchangeUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ExchangeUtils.class);

    private ExchangeUtils() {
        throw new UnsupportedOperationException("This class cannot be instantiated");
    }

    /**
     * Copies variables from Camel into the process engine.
     * 
     * This method will conditionally copy the Camel body to the "camelBody"
     * variable if it is of type java.lang.String, OR it will copy the Camel
     * body to individual variables within the process engine if it is of type
     * Map. If the copyVariablesFromProperties parameter is set
     * on the endpoint, the properties are copied instead
     * 
     * @param exchange
     *            The Camel Exchange object
     * @param parameters
     *        Parameters as defined in the docs
     * @return A Map containing all the variables to be used
     *         in the process engine
     */
    public static Map<String, Object> prepareVariables(Exchange exchange, Map<String, Object> parameters) {
        Map<String, Object> processVariables = new HashMap<>();
        Object camelBody = exchange.getIn().getBody();
        if (camelBody instanceof String) {
            // If the COPY_MESSAGE_BODY_AS_PROCESS_VARIABLE_PARAMETER was passed
            // the value of it
            // is taken as variable to store the (string) body in
            String processVariableName = "camelBody";
            if (parameters.containsKey(CamundaBpmConstants.COPY_MESSAGE_BODY_AS_PROCESS_VARIABLE_PARAMETER)) {
                processVariableName = (String) parameters.get(
                        CamundaBpmConstants.COPY_MESSAGE_BODY_AS_PROCESS_VARIABLE_PARAMETER);
            }
            processVariables.put(processVariableName, camelBody);
        } else if (camelBody instanceof Map<?, ?> camelBodyMap) {
            for (Map.Entry<?, ?> e : camelBodyMap.entrySet()) {
                if (e.getKey() instanceof String) {
                    processVariables.put((String) e.getKey(), e.getValue());
                }
            }
        } else if (camelBody != null && LOG.isWarnEnabled()) {
            LOG.warn(
                "unknown type of camel body - not handed over to process engine: {}",
                camelBody.getClass());
        }
        return processVariables;
    }
}
