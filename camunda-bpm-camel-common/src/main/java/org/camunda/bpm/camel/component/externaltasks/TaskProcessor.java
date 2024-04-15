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

import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_ATTEMPTSSTARTED;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_RETRIESLEFT;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_TASK;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_TASKID;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_RESPONSE_IGNORE;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import org.apache.camel.*;
import org.apache.camel.spi.Synchronization;
import org.camunda.bpm.camel.common.CamundaUtils;
import org.camunda.bpm.camel.component.CamundaBpmEndpoint;
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.externaltask.ExternalTask;
import org.camunda.bpm.engine.externaltask.LockedExternalTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskProcessor implements Processor {
    private static final Logger LOG = LoggerFactory.getLogger(TaskProcessor.class.getCanonicalName());

    private final CamundaBpmEndpoint camundaEndpoint;

    // parameters
    private final int retries;
    private final long retryTimeout;
    private final long[] retryTimeouts;
    private final boolean completeTask;
    private final boolean onCompletion;
    private final String topic;
    private final String workerId;

    public TaskProcessor(final CamundaBpmEndpoint endpoint, final String topic, final int retries,
            final long retryTimeout, final long[] retryTimeouts, final boolean completeTask, final boolean onCompletion,
            final String workerId) {
        this.camundaEndpoint = endpoint;
        this.retries = retries;
        this.retryTimeout = retryTimeout;
        this.retryTimeouts = retryTimeouts;
        this.completeTask = completeTask;
        this.onCompletion = onCompletion;
        this.workerId = workerId;
        this.topic = topic;
    }

    private ExternalTaskService getExternalTaskService() {
        return camundaEndpoint.getProcessEngine().getExternalTaskService();
    }

    @Override
    public void process(final Exchange exchange) {
        if (!onCompletion) {
            internalProcessing(exchange);
        } else {
            // set headers only for "on-completion" processing because
            // otherwise the service doing something is already done
            // and therefore cannot use this in-headers anymore.
            setInHeaders(exchange);
            final TaskProcessor taskProcessor = this;
            exchange.getExchangeExtension().addOnCompletion(new Synchronization() {
                @Override
                public void onFailure(final Exchange exchange) {
                    taskProcessor.internalProcessing(exchange);
                }

                @Override
                public void onComplete(final Exchange exchange) {
                    taskProcessor.internalProcessing(exchange);
                }
            });
        }
    }

    private void setInHeaders(final Exchange exchange) {
        final Message in = getInMessage(exchange);
        final String taskId = getExternalTaskId(in);
        final ExternalTask task = getExternalTask(taskId);

        final SetExternalTaskRetries annotation = findAnnotationByException(exchange.getException());
        final int retriesLeft = retriesLeft(task.getRetries(), annotation);
        final int attemptsStarted = attemptsStarted(task.getRetries(), annotation);

        in.setHeader(EXCHANGE_HEADER_RETRIESLEFT, retriesLeft);
        in.setHeader(EXCHANGE_HEADER_ATTEMPTSSTARTED, attemptsStarted);
    }

    @SuppressWarnings("unchecked")
    void internalProcessing(final Exchange exchange) {
        final Message in = getInMessage(exchange);
        String currentTaskId;
        ExternalTask currentTask;
        try {
            currentTaskId = getExternalTaskId(in);
            currentTask = getExternalTask(currentTaskId);
        } catch (final NoSuchExternalTaskException e) {
            currentTask = null;
            currentTaskId = null;
        }
        final ExternalTask task = currentTask;
        final String taskId = currentTaskId;

        final ExternalTaskService externalTaskService = getExternalTaskService();

        final Message message;
        if (onCompletion) {
            // 'process-externalTask' at the end of a route does not have any
            // output
            // - only input which was any proceeder's output.
            message = exchange.getMessage();
        } else {
            message = in;
        }

        // failure
        if (exchange.isFailed()) {
            if (task == null) {
                LOG.warn(
                    "Processing failed but the task seems to be already processed - will do nothing!" +
                        " Camunda external task id: '{}", taskId);
                return;
            }
            final Exception exception = exchange.getException();
            final SetExternalTaskRetries annotation = findAnnotationByException(exchange.getException());
            final int retriesLeft = retriesLeft(task.getRetries(), annotation);
            final long calculatedTimeout = calculateTimeout(task.getRetries(), annotation);
            CamundaUtils.retryIfOptimisticLockingException((Callable<Void>) () -> {
                externalTaskService.handleFailure(task.getId(),
                        task.getWorkerId(),
                        exception != null ? exception.getMessage() : "task failed",
                        retriesLeft,
                        calculatedTimeout);
                return null;
            });
        } else if (completeTask) {
            if ((message != null) && (message.getBody() != null) && (message.getBody() instanceof String)) {
                // bpmn error
                final String errorCode = message.getBody(String.class);
                if (task == null) {
                    LOG.warn("Should complete the external task with BPM error '{}'" +
                        " but the task seems to be already processed - will do nothing!" +
                        " Camunda external task id: '{}'", errorCode, taskId);
                    return;
                }

                // Ignore if service advises us to do so.
                if (errorCode.equals(EXCHANGE_RESPONSE_IGNORE)) {
                    return;
                }

                CamundaUtils.retryIfOptimisticLockingException((Callable<Void>) () -> {
                  externalTaskService.handleBpmnError(task.getId(), task.getWorkerId(), errorCode);
                  return null;
                });
            } else {
                // success
                if (task == null) {
                    LOG.warn(
                        "Should complete the external task but the task seems to be already processed - will do nothing!" +
                            " Camunda external task id: '{}'",
                        taskId);
                    return;
                }

                final Map<String, Object> variablesToBeSet;
                if ((message != null) && (message.getBody() != null) && (message.getBody() instanceof Map)) {
                    variablesToBeSet = message.getBody(Map.class);
                } else {
                    variablesToBeSet = null;
                }

                CamundaUtils.retryIfOptimisticLockingException((Callable<Void>) () -> {
                  if (variablesToBeSet != null) {
                    externalTaskService.complete(task.getId(), task.getWorkerId(), variablesToBeSet);
                  } else {
                    externalTaskService.complete(task.getId(), task.getWorkerId());
                  }
                  return null;
                });

            }
        }
    }

    private Message getInMessage(final Exchange exchange) {
        final Message in = exchange.getIn();
        if (in == null) {
            throw new RuntimeCamelException("Unexpected exchange: in is null!");
        }
        return in;
    }

    private ExternalTask getExternalTask(final String taskId) {
        if (taskId == null) {
            return null;
        }

        final ExternalTask task = getExternalTaskService().createExternalTaskQuery().externalTaskId(
                taskId).singleResult();
        if (task != null) {
            if ((task.getWorkerId() != null) && (workerId != null) && !task.getWorkerId().equals(workerId)) {
                throw new RuntimeCamelException(
                        "Unexpected exchange: the external task '" + taskId + "' is locked for worker '"
                                + task.getWorkerId() + "' which differs from the configured worker '" + workerId + "!");
            }
            if ((task.getTopicName() != null) && (topic != null) && !task.getTopicName().equals(topic)) {
                throw new RuntimeCamelException(
                        "Unexpected exchange: the external task '" + taskId + "' is from topic '" + task.getWorkerId()
                                + "' which differs from the configured topic '" + topic + "!");
            }
        } else {
            throw new NoSuchExternalTaskException("The referenced external task '" + taskId
                    + "' is not available any more. For details see '"
                    + "https://github.com/camunda/camunda-bpm-camel#camunda-bpmasync-externaltask-processing-outstanding-external-tasks'.");
        }
        return task;
    }

    private String getExternalTaskId(final Message in) {
        final LockedExternalTask lockedTask = in.getHeader(EXCHANGE_HEADER_TASK, LockedExternalTask.class);
        final String lockedTaskId = in.getHeader(EXCHANGE_HEADER_TASKID, String.class);

        return Optional.ofNullable(lockedTask)
            .map(LockedExternalTask::getId)
            .orElse(lockedTaskId);
    }

    public int retriesLeft(final Integer taskRetries, final SetExternalTaskRetries annotation) {
        final int currentRetries;
        final boolean decreaseCurrentRetriesByOne;
        if (taskRetries == null) {
            currentRetries = this.retries;
            decreaseCurrentRetriesByOne = false;
        } else {
            currentRetries = taskRetries;
            decreaseCurrentRetriesByOne = true;
        }
        return findRetriesByAnnotation(annotation, currentRetries, decreaseCurrentRetriesByOne);
    }

    private SetExternalTaskRetries findAnnotationByException(final Throwable e) {
        if (e == null) {
            return null;
        }
        final SetExternalTaskRetries annotation = e.getClass().getAnnotation(SetExternalTaskRetries.class);
        if (annotation != null) {
            return annotation;
        }
        return findAnnotationByException(e.getCause());
    }

    private int findRetriesByAnnotation(final SetExternalTaskRetries annotation, final int currentRetries,
            final boolean decreaseCurrentRetriesByOne) {
        if (annotation == null) {
            if (decreaseCurrentRetriesByOne) {
                return currentRetries - 1;
            } else {
                return currentRetries;
            }
        }

        if (annotation.relative()) {
            return currentRetries + annotation.retries();
        } else {
            return annotation.retries();
        }
    }

    public int attemptsStarted(final Integer taskRetries, final SetExternalTaskRetries annotation) {
        final int retriesLeft = retriesLeft(taskRetries, annotation);
        return this.retries - retriesLeft;
    }

    private long calculateTimeout(final Integer taskRetries, final SetExternalTaskRetries annotation) {
        final int currentTry = attemptsStarted(taskRetries, annotation);
        if (retries < 1) {
            return 0;
        } else if ((retryTimeouts != null) && (currentTry < retryTimeouts.length)) {
            return retryTimeouts[currentTry];
        } else {
            return retryTimeout;
        }
    }
}
