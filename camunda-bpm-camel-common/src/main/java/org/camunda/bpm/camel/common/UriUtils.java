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

public class UriUtils {

    public static class ParsedUri {

        private final String remainingUri;

        private final UriType type;

        private final String[] components;

        /**
         * @param remainingUri
         *            the remaining part of the URI without the query parameters
         *            or component prefix
         */
        public ParsedUri(final String remainingUri) {
            this.remainingUri = remainingUri;
            components = parseUri(remainingUri);
            if (components.length == 0) {
                throw new RuntimeException("Cannot create a producer for URI '" + remainingUri + "'");
            }
            final String identifier = components[0];
            type = UriType.typeByIdentifier(identifier);
        }

        public String[] getComponents() {
            return components;
        }

        public UriType getType() {
            return type;
        }
        /**
         * @return the remaining part of the URI without the query parameters or
         *         component prefix
         */
        public String getRemainingUri() {
            return remainingUri;
        }
    }

    public enum UriType {
        START_PR("start"),
        SEND_SIGNAL("signal"),
        SEND_MESSAGE("message"),
        POLL_EXTERNAL_TASKS("poll-externalTasks"),
        PROCESS_EXTERNAL_TASK("async-externalTask");

        private final String identifier;

        UriType(final String identifier) {
            this.identifier = identifier;
        }

        public static UriType typeByIdentifier(final String identifier) {
            for (final UriType type : values()) {
                if (type.identifier.equals(identifier)) {
                    return type;
                }
            }
            throw new RuntimeException("Cannot create a producer for identifier '" + identifier + "'");
        }

    }

    public static String[] parseUri(String remainingUri) {
        return remainingUri.split("/");
    }
}
