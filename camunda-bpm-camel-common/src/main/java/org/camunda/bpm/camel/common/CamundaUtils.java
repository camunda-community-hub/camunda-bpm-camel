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

import java.util.concurrent.Callable;

import org.camunda.bpm.engine.OptimisticLockingException;

public class CamundaUtils {
    private static final long SLEEP_IN_MS = 250;
    private static final int DEFAULT_TIMES = 1000;

    private CamundaUtils() {
        throw new UnsupportedOperationException("This class cannot be instantiated");
    }

    public static <V> V retryIfOptimisticLockingException(final Callable<V> action) {
        return retryIfOptimisticLockingException(DEFAULT_TIMES, action);
    }

    public static <V> V retryIfOptimisticLockingException(int times, final Callable<V> action) {
        OptimisticLockingException lastException;
        do {
            try {
                return action.call();
            } catch (OptimisticLockingException e) {
                lastException = e;
                --times;
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            try {
                Thread.sleep(SLEEP_IN_MS);
            } catch (InterruptedException e) {
                // never mind
            }
        } while (times > 0);
      throw new OptimisticLockingException(
          "Event after " + times + " attempts (every delayed for " + SLEEP_IN_MS + "ms)" +
              " an OptimisticLockingException is thrown!" +
              " message='" + lastException.getMessage() + '\'');
    }

}
