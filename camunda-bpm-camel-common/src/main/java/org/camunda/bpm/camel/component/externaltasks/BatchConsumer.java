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

import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_DEFINITION_ID;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_DEFINITION_KEY;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_PRIO;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_TASK;

import java.lang.reflect.Method;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.camel.*;
import org.apache.camel.support.ScheduledBatchPollingConsumer;
import org.apache.camel.util.CastUtils;
import org.camunda.bpm.camel.common.CamundaUtils;
import org.camunda.bpm.camel.component.CamundaBpmEndpoint;
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.externaltask.ExternalTaskQueryTopicBuilder;
import org.camunda.bpm.engine.externaltask.LockedExternalTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchConsumer extends ScheduledBatchPollingConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(BatchConsumer.class);

    private final CamundaBpmEndpoint camundaEndpoint;

    // parameters
    private int timeout;
    private final long lockDuration;
    private final String topic;
    private final boolean completeTask;
    private final List<String> variablesToFetch;
    private final boolean deserializeVariables;
    private static Method deserializeVariablesMethod;
    private final String workerId;

    private final TaskProcessor taskProcessor;

    static {
        try {
            deserializeVariablesMethod = ExternalTaskQueryTopicBuilder.class.getMethod(
                    "enableCustomObjectDeserialization");
        } catch (Exception e) {
            // ignore because the Camunda version below 7.6.0 is used
        }
    }

    public static boolean systemKnowsDeserializationOfVariables() {
        return deserializeVariablesMethod != null;
    }

    public BatchConsumer(final CamundaBpmEndpoint endpoint, final Processor processor, final int retries,
            final long retryTimeout, final long[] retryTimeouts, final long lockDuration, final String topic,
            final boolean completeTask, final List<String> variablesToFetch, final boolean deserializeVariables,
            final String workerId) {
        super(endpoint, processor);
        this.camundaEndpoint = endpoint;
        this.lockDuration = lockDuration;
        this.topic = topic;
        this.completeTask = completeTask;
        this.variablesToFetch = variablesToFetch;
        this.deserializeVariables = deserializeVariables;
        this.workerId = workerId;
        this.taskProcessor = new TaskProcessor(endpoint,
                topic,
                retries,
                retryTimeout,
                retryTimeouts,
                completeTask,
                true,
                workerId);
    }

    public BatchConsumer(final CamundaBpmEndpoint endpoint, final Processor processor,
            final ScheduledExecutorService executor, final int retries, final long retryTimeout,
            final long[] retryTimeouts, final long lockDuration, final String topic, final boolean completeTask,
            final List<String> variablesToFetch, final boolean deserializeVariables, final String workerId) {
        super(endpoint, processor, executor);
        this.camundaEndpoint = endpoint;
        this.lockDuration = lockDuration;
        this.topic = topic;
        this.completeTask = completeTask;
        this.variablesToFetch = variablesToFetch;
        this.deserializeVariables = deserializeVariables;
        this.workerId = workerId;
        this.taskProcessor = new TaskProcessor(endpoint,
                topic,
                retries,
                retryTimeout,
                retryTimeouts,
                completeTask,
                true,
                workerId);
    }

    @Override
    public void close() {
        LOG.info("Closing BatchConsumer");
        this.stop();
    }

    @Override
    public int processBatch(Queue<Object> exchanges) throws Exception {
        int total = exchanges.size();
        int answer = total;

        for (int index = 0; index < total && isBatchAllowed(); index++) {
            // only loop if we are started (allowed to run)
            // use poll to remove the head, so it does not consume memory even
            // after we have processed it
            Exchange exchange = (Exchange) exchanges.poll();
            // add current index and total as properties
            exchange.setProperty(Exchange.BATCH_INDEX, index);
            exchange.setProperty(Exchange.BATCH_SIZE, total);
            exchange.setProperty(Exchange.BATCH_COMPLETE, index == total - 1);

            // update pending number of exchanges
            pendingExchanges = total - index - 1;

            boolean started = processExchange(exchange);

            // if we did not start processing then decrement the counter
            if (!started) {
                answer--;
            }
        }

        // drain any in progress files as we are done with this batch
        removeExcessiveInProgressTasks(CastUtils.cast(exchanges, Exchange.class), 0);

        return answer;
    }

    private boolean processExchange(final Exchange exchange) throws Exception {
        taskProcessor.process(exchange);
        final Processor currentProcessor = getProcessor();
        if (currentProcessor instanceof AsyncProcessor asyncProcessor) {
            asyncProcessor.process(exchange, doneSync -> {
                // we are not interested in this event
            });
        } else {
            currentProcessor.process(exchange);
        }
        return true;
    }

    /**
     * Drain any in progress files as we are done with this batch
     *
     * @param exchanges
     *            the exchanges
     * @param limit
     *            the limit
     */
    protected void removeExcessiveInProgressTasks(Queue<Exchange> exchanges, int limit) {
        while (exchanges.size() > limit) {
            // must remove last
            Exchange exchange = exchanges.poll();
            releaseTask(exchange);
        }
    }

    private void releaseTask(final Exchange exchange) {
        exchange.setRollbackOnly(true);
        taskProcessor.internalProcessing(exchange);
    }

    private ExternalTaskService getExternalTaskService() {
        return camundaEndpoint.getProcessEngine().getExternalTaskService();
    }

    protected int poll() throws Exception {
        int messagesPolled = 0;
        PriorityQueue<Exchange> exchanges = new PriorityQueue<>(
                /*
                 * default-value, unfortunately
                 * PriorityQueue.DEFAULT_INITIAL_CAPACITY is private and the
                 * constructor with one parameter of type Comparator is not
                 * available in JSE 7
                 */
                11, (o1, o2) -> {
                    Long priority1 =
                        o1.getProperty(EXCHANGE_HEADER_PROCESS_PRIO, 0, Long.class);
                    Long priority2 =
                        o2.getProperty(EXCHANGE_HEADER_PROCESS_PRIO, 0, Long.class);
                    return priority1.compareTo(priority2);
                });

        if (isPollAllowed()) {
            final List<LockedExternalTask> tasks = CamundaUtils.retryIfOptimisticLockingException(
                () -> {
                    ExternalTaskQueryTopicBuilder query = getExternalTaskService() //
                            .fetchAndLock(maxMessagesPerPoll, workerId, true) //
                            .topic(topic, lockDuration) //
                            .variables(variablesToFetch);
                    if (deserializeVariables && (deserializeVariablesMethod != null)) {
                        try {
                            query = (ExternalTaskQueryTopicBuilder) deserializeVariablesMethod.invoke(query);
                        } catch (Exception e) {
                            throw new RuntimeCamelException(e);
                        }
                    }
                    return query.execute();
                });

            messagesPolled = tasks.size();

            for (final LockedExternalTask task : tasks) {
                final ExchangePattern pattern = completeTask ? ExchangePattern.InOut : ExchangePattern.InOnly;
                Exchange exchange = getEndpoint().createExchange(pattern);
                exchange.getExchangeExtension().setFromEndpoint(getEndpoint());
                exchange.setExchangeId(task.getWorkerId() + "/" + task.getId());
                exchange.setProperty(EXCHANGE_HEADER_PROCESS_INSTANCE_ID, task.getProcessInstanceId());
                exchange.setProperty(EXCHANGE_HEADER_PROCESS_DEFINITION_KEY, task.getProcessDefinitionKey());
                exchange.setProperty(EXCHANGE_HEADER_PROCESS_DEFINITION_ID, task.getProcessDefinitionId());
                exchange.setProperty(EXCHANGE_HEADER_PROCESS_PRIO, task.getPriority());

                final Message in = exchange.getIn();
                in.setHeader(EXCHANGE_HEADER_TASK, task);
                in.setBody(task.getVariables());
                exchanges.add(exchange);
            }
        }
        processBatch(CastUtils.cast(exchanges));
        return messagesPolled;
    }

    public int getTimeout() {
        return timeout;
    }

    /**
     * Sets a timeout to use with {@link PollingConsumer}. <br>
     * <br>
     * Use <b>timeout &lt; 0</b> for {@link PollingConsumer#receive()}. <br>
     * Use <b>timeout == 0</b> for {@link PollingConsumer#receiveNoWait()}.
     * <br>
     * Use <b>timeout &gt; 0</b> for {@link PollingConsumer#receive(long)}}.
     * <br>
     * The default timeout value is <b>0</b>
     *
     * @param timeout
     *            the timeout value
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
}
