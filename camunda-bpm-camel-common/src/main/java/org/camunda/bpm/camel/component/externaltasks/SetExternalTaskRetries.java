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
package org.camunda.bpm.camel.component.externaltasks;

import java.lang.annotation.Target;
import java.lang.annotation.Retention;
import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotate an exception by &quot;SetExternalTaskRetries&quot; to force setting
 * Camunda's external task's retry counter to a value given by the annotation.
 * <p>
 * Example:
 * <ul>
 * <li>{@link SetExternalTaskRetries}(retries = -1, relative = true) // the
 * default behavior if no annotation is given
 * <li>{@link SetExternalTaskRetries}(retries = 0) // the force a Camunda
 * incident since the retry counter is 0
 * <li>{@link SetExternalTaskRetries}(retries = 0, relative = true) // don't
 * change the retry counter although an exception is thrown
 * </ul>
 */
@Target(value = { ElementType.TYPE })
@Retention(value = RetentionPolicy.RUNTIME)
public @interface SetExternalTaskRetries {
    int retries();
    boolean relative() default false;
}
