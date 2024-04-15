package org.camunda.bpm.camel.cdi;

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

import java.io.Serial;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Expression;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.camunda.bpm.camel.component.CamundaBpmConstants;
import org.camunda.bpm.camel.component.externaltasks.SetExternalTaskRetries;
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.externaltask.ExternalTask;
import org.camunda.bpm.engine.externaltask.LockedExternalTask;
import org.camunda.bpm.engine.history.HistoricProcessInstance;
import org.camunda.bpm.engine.history.HistoricVariableInstance;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.h2.H2DatabaseTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@QuarkusTestResource(H2DatabaseTestResource.class)
public class ConsumeExternalTasksTest extends BaseQuarkusIntegrationTest {
    @Inject
    CamelContext camelContext;
    @Inject
    ExternalTaskService externalTaskService;
    @Inject
    @EndpointInject("mock:resultEndpointET")
    MockEndpoint mockEndpoint;

    @Produces
    @ApplicationScoped
    public RouteBuilder createRoute() {
        return new RouteBuilder() {
            public void configure() {
                from("camunda-bpm:poll-externalTasks?topic=topic1&maxTasksPerPoll=5&delay=250&retries=2&retryTimeouts=1s&retryTimeout=2s")
                        .routeId("firstRoute")
                        .to("log:org.camunda.bpm.camel.cdi?level=INFO&showAll=true&multiline=true")
                        .threads(2)
                        .to(mockEndpoint)
                ;

                from("camunda-bpm:poll-externalTasks?topic=topic2&async=true&delay=250&variablesToFetch=var2,var3&lockDuration=2s&maxTasksPerPoll=2&workerId=0815")
                        .routeId("secondRoute")
                        .to("log:org.camunda.bpm.camel.cdi?level=INFO&showAll=true&multiline=true")
                        .to(mockEndpoint)
                ;

                from("camunda-bpm:poll-externalTasks?topic=topic3&async=false&delay=250&variablesToFetch=")
                        .routeId("thirdRoute")
                        .to("log:org.camunda.bpm.camel.cdi?level=INFO&showAll=true&multiline=true")
                        .to(mockEndpoint)
                ;

                from("direct:firstTestRoute")
                        .routeId("firstProcessRoute")
                        .to("camunda-bpm:async-externalTask?onCompletion=true&retries=2&retryTimeouts=1s")
                        .to(mockEndpoint)
                ;

                from("direct:secondTestRoute")
                        .routeId("secondProcessRoute")
                        .to(mockEndpoint)
                        .to("camunda-bpm:async-externalTask")
                ;
            }
        };
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSetProcessVariables() throws Exception {
        deployProcess("process/StartExternalTask.bpmn20.xml");
        // variables to be set by the Camel-endpoint processing the external
        // task
        mockEndpoint.returnReplyBody(new Expression() {
            @Override
            public <T> T evaluate(Exchange exchange, Class<T> type) {
                Map<String, Object> variables = exchange.getIn().getBody(Map.class);
                final String var2 = (String) variables.get("var2");

                final HashMap<String, Object> result = new HashMap<>();
                result.put("var2", var2 + "bar");
                result.put("var3", "bar3");

                return (T) result;
            }
        });

        // start process
        final Map<String, Object> processVariables = new HashMap<>();
        processVariables.put("var1", "foo");
        processVariables.put("var2", "bar");
        final ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("startExternalTaskProcess",
                processVariables);
        assertNotNull(processInstance);
        await().atMost(1, TimeUnit.SECONDS).until(
                () -> externalTaskService.createExternalTaskQuery().processInstanceId(processInstance.getId()).active().count() == 0);

        // assert that the camunda BPM process instance ID has been added as a
        // property to the message
        assertEquals(processInstance.getId(), mockEndpoint.assertExchangeReceived(0).getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID));

        // all process instance variables are loaded since no "variablesToFetch"
        // parameter was given
        var exchangeIn = mockEndpoint.assertExchangeReceived(0).getIn();
        assertNotNull(exchangeIn.getBody());
        assertInstanceOf(Map.class, exchangeIn.getBody());
        assertEquals(2, exchangeIn.getBody(Map.class).size());
        assertEquals("foo", exchangeIn.getBody(Map.class).get("var1"));
        assertEquals("bar", exchangeIn.getBody(Map.class).get("var2"));

        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        assertEquals(3, variables.size());
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        assertAll(
                () -> assertEquals("foo", variablesAsMap.get("var1")),
                () -> assertEquals("barbar", variablesAsMap.get("var2")),
                () -> assertEquals("bar3", variablesAsMap.get("var3"))
        );

        assertHappyEnd(processInstance);

        // assert that process ended not due to error boundary event 4711
        assertProcessDidNotEnd(processInstance, "End4711");

        // assert that process ended not due to error boundary event 0815
        assertProcessDidNotEnd(processInstance, "End0815");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLoadNoProcessVariablesAndAsyncIsFalse() throws Exception {
        deployProcess("process/StartExternalTask3.bpmn20.xml");
        // variables to be set by the Camel-endpoint processing the external
        // task
        mockEndpoint.returnReplyBody(new Expression() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> T evaluate(Exchange exchange, Class<T> type) {
                final HashMap<String, Object> result = new HashMap<>();
                result.put("var1", "foo1");
                result.put("var2", "bar2");
                return (T) result;
            }
        });

        // start process
        final Map<String, Object> processVariables = new HashMap<>();
        processVariables.put("var1", "foo");
        processVariables.put("var2", "bar");
        final ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("startExternalTaskProcess3",
                processVariables);
        assertNotNull(processInstance);
        await().atMost(1, TimeUnit.SECONDS).until(
                () -> externalTaskService.createExternalTaskQuery().processInstanceId(processInstance.getId()).active().count() == 0);


        // assert that the camunda BPM process instance ID has been added as a
        // property to the message
        assertEquals(processInstance.getId(), mockEndpoint.assertExchangeReceived(0).getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID));

        // no process instance variables are loaded since an empty
        // "variablesToFetch" parameter was given
        assertNotNull(mockEndpoint.assertExchangeReceived(0).getIn().getBody());
        assertInstanceOf(Map.class, mockEndpoint.assertExchangeReceived(0).getIn().getBody());
        assertTrue(mockEndpoint.assertExchangeReceived(0).getIn().getBody(Map.class).isEmpty());

        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        assertEquals(2, variables.size());
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        assertAll(
                () -> assertEquals("foo1", variablesAsMap.get("var1")),
                () -> assertEquals("bar2", variablesAsMap.get("var2"))
        );

        // assert that process in end event "HappyEnd"
        assertHappyEnd(processInstance);

        // assert that process ended not due to error boundary event 4711
        assertProcessDidNotEnd(processInstance, "End4711");

        // assert that process ended not due to error boundary event 0815
        assertProcessDidNotEnd(processInstance, "End0815");
    }

    @Test
    public void testCompleteTaskOnCompletionSuccessfully() {
        deployProcess("process/StartExternalTask4.bpmn20.xml");
        // variables to be set by the Camel-endpoint processing the external
        // task
        mockEndpoint.returnReplyBody(new Expression() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> T evaluate(Exchange exchange, Class<T> type) {
                final HashMap<String, Object> result = new HashMap<>();
                result.put("var2", "bar2");
                result.put("var3", "bar3");
                return (T) result;
            }
        });

        // start process
        final Map<String, Object> processVariables = new HashMap<>();
        processVariables.put("var1", "foo");
        processVariables.put("var2", "bar");
        processVariables.put("var3", "foobar");
        final ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("startExternalTaskProcess2",
                processVariables);
        assertNotNull(processInstance);

        // external task is still not resolved and not locked
        final List<ExternalTask> externalTasks1 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks1);
        assertEquals(1, externalTasks1.size());
        assertNull(externalTasks1.get(0).getWorkerId());

        // find external task and lock
        final List<LockedExternalTask> locked = externalTaskService.fetchAndLock(1, "0815", true).topic("topic4", 5000).execute();
        assertNotNull(locked);
        assertEquals(1, locked.size());
        final LockedExternalTask lockedExternalTask = locked.get(0);

        // call route "direct:testRoute"
        final ProducerTemplate template = camelContext.createProducerTemplate();
        template.requestBodyAndHeader("direct:firstTestRoute",
                null,
                "CamundaBpmExternalTaskId",
                lockedExternalTask.getId());

        // ensure endpoint has been called
        assertNotNull(mockEndpoint.assertExchangeReceived(0));
        assertEquals(0, mockEndpoint
                .assertExchangeReceived(0)
                .getIn()
                .getHeader(CamundaBpmConstants.EXCHANGE_HEADER_ATTEMPTSSTARTED));
        assertEquals(2, mockEndpoint
                .assertExchangeReceived(0)
                .getIn()
                .getHeader(CamundaBpmConstants.EXCHANGE_HEADER_RETRIESLEFT));

        // assert that process in end event "HappyEnd"
        assertHappyEnd(processInstance);

        // assert that process ended not due to error boundary event 4711
        assertProcessDidNotEnd(processInstance, "End4711");

        // assert that process ended not due to error boundary event 0815
        assertProcessDidNotEnd(processInstance, "End0815");

        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        assertEquals(3, variables.size());
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        assertAll(
                () -> assertEquals("foo", variablesAsMap.get("var1")),
                () -> assertEquals("bar2", variablesAsMap.get("var2")),
                () -> assertEquals("bar3", variablesAsMap.get("var3"))
        );
    }

    @Test
    public void testCompleteTaskOnCompletionFailure() {
        deployProcess("process/StartExternalTask4.bpmn20.xml");
        final String FAILURE = "Failure";

        // variables returned but must not be set since task will not be
        // completed
        mockEndpoint.whenAnyExchangeReceived(exchange -> {
            throw new Exception(FAILURE);
        });

        // start process
        final Map<String, Object> processVariables = new HashMap<>();
        processVariables.put("var1", "foo");
        processVariables.put("var2", "bar");
        processVariables.put("var3", "foobar");
        final ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("startExternalTaskProcess2",
                processVariables);
        assertNotNull(processInstance);

        // external task is still not resolved and not locked
        final List<ExternalTask> externalTasks1 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks1);
        assertEquals(1, externalTasks1.size());
        assertNull(externalTasks1.get(0).getWorkerId());
        assertNull(externalTasks1.get(0).getRetries());

        // find external task and lock
        final List<LockedExternalTask> locked = externalTaskService.fetchAndLock(1, "0815", true).topic("topic4", 5000).execute();
        assertNotNull(locked);
        assertEquals(1, locked.size());
        final LockedExternalTask lockedExternalTask = locked.get(0);

        // call route "direct:testRoute"
        final ProducerTemplate template = camelContext.createProducerTemplate();
        try {
            template.requestBodyAndHeader("direct:firstTestRoute",
                    null,
                    "CamundaBpmExternalTaskId",
                    lockedExternalTask.getId());
            fail("Expected an exception, but Camel route succeeded!");
        } catch (Exception e) {
            // expected
        }

        // ensure endpoint has been called
        assertNotNull(mockEndpoint.assertExchangeReceived(0));

        // external task is still not resolved
        final List<ExternalTask> externalTasks2 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks2);
        assertEquals(1, externalTasks2.size());
        assertEquals(2, externalTasks2.get(0).getRetries());

        // assert that process not in the end event "HappyEnd"
        assertProcessDidNotEnd(processInstance, "HappyEnd");

        // assert that process ended not due to error boundary event 4711
        assertProcessDidNotEnd(processInstance, "End4711");

        // assert that process ended not due to error boundary event 0815
        assertProcessDidNotEnd(processInstance, "End0815");

        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        assertEquals(3, variables.size());
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        assertAll(
                () -> assertEquals("foo", variablesAsMap.get("var1")),
                () -> assertEquals("bar", variablesAsMap.get("var2")),
                () -> assertEquals("foobar", variablesAsMap.get("var3"))
        );

        // complete task to make test order not relevant
        externalTaskService.complete(externalTasks2.get(0).getId(), "0815");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCompleteTaskSuccessfully() {
        deployProcess("process/StartExternalTask4.bpmn20.xml");
        // variables to be set by the Camel-endpoint processing the external
        // task
        mockEndpoint.returnReplyBody(new Expression() {
            @Override
            public <T> T evaluate(Exchange exchange, Class<T> type) {
                final HashMap<String, Object> result = new HashMap<>();
                result.put("var2", "bar2");
                result.put("var3", "bar3");
                return (T) result;
            }
        });

        // start process
        final Map<String, Object> processVariables = new HashMap<>();
        processVariables.put("var1", "foo");
        processVariables.put("var2", "bar");
        processVariables.put("var3", "foobar");
        final ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("startExternalTaskProcess2",
                processVariables);
        assertNotNull(processInstance);

        // external task is still not resolved and not locked
        final List<ExternalTask> externalTasks1 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks1);
        assertEquals(1, externalTasks1.size());
        assertNull(externalTasks1.get(0).getWorkerId());
        assertNull(externalTasks1.get(0).getRetries());

        // find external task and lock
        final List<LockedExternalTask> locked = externalTaskService.fetchAndLock(1, "0815", true).topic("topic4", 5000).execute();
        assertNotNull(locked);
        assertEquals(1, locked.size());
        final LockedExternalTask lockedExternalTask = locked.get(0);

        // call route "direct:testRoute"
        final ProducerTemplate template = camelContext.createProducerTemplate();
        template.requestBodyAndHeader("direct:secondTestRoute",
                null,
                "CamundaBpmExternalTaskId",
                lockedExternalTask.getId());

        // ensure endpoint has been called
        assertNotNull(mockEndpoint.assertExchangeReceived(0));

        // assert that process in end event "HappyEnd"
        assertHappyEnd(processInstance);

        // assert that process ended not due to error boundary event 4711
        assertProcessDidNotEnd(processInstance, "End4711");

        // assert that process ended not due to error boundary event 0815
        assertProcessDidNotEnd(processInstance, "End0815");

        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        assertEquals(3, variables.size());
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        assertAll(
                () -> assertEquals("foo", variablesAsMap.get("var1")),
                () -> assertEquals("bar2", variablesAsMap.get("var2")),
                () -> assertEquals("bar3", variablesAsMap.get("var3"))
        );
    }

    @Test
    public void testCompleteTaskFailure() {
        deployProcess("process/StartExternalTask4.bpmn20.xml");
        // start process
        final Map<String, Object> processVariables = new HashMap<>();
        processVariables.put("var1", "foo");
        processVariables.put("var2", "bar");
        processVariables.put("var3", "foobar");
        final ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("startExternalTaskProcess2",
                processVariables);
        assertNotNull(processInstance);

        // external task is still not resolved and not locked
        final List<ExternalTask> externalTasks1 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks1);
        assertEquals(1, externalTasks1.size());
        assertNull(externalTasks1.get(0).getWorkerId());
        assertNull(externalTasks1.get(0).getRetries());

        // find external task and lock
        final List<LockedExternalTask> locked = externalTaskService.fetchAndLock(1, "0815", true).topic("topic4", 5000).execute();
        assertNotNull(locked);
        assertEquals(1, locked.size());
        final LockedExternalTask lockedExternalTask = locked.get(0);


        /*
         * test CamundaBpmConstants.EXCHANGE_RESPONSE_IGNORE
         */

        // variables returned but must not be set since task will not be
        // completed
        mockEndpoint.returnReplyBody(new Expression() {
            @SuppressWarnings("unchecked")
            @Override
            public <T> T evaluate(Exchange exchange, Class<T> type) {
                return (T) CamundaBpmConstants.EXCHANGE_RESPONSE_IGNORE;
            }
        });

        // second route does not recognize exceptions

        // call route "direct:secondTestRoute"
        final ProducerTemplate template = camelContext.createProducerTemplate();
        template.requestBodyAndHeader("direct:secondTestRoute",
                null,
                "CamundaBpmExternalTaskId",
                lockedExternalTask.getId());

        // ensure endpoint has been called
        assertNotNull(mockEndpoint.assertExchangeReceived(0));

        // external task is still not resolved
        final List<ExternalTask> externalTasks2 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks2);
        assertEquals(1, externalTasks2.size());
        // Exception aborted processing so retries could not be set!
        assertNull(externalTasks2.get(0).getRetries());
        assertNull(externalTasks2.get(0).getErrorMessage());

        // assert that process not in the end event "HappyEnd"
        assertProcessDidNotEnd(processInstance, "HappyEnd");

        // assert that process ended not due to error boundary event 4711
        assertProcessDidNotEnd(processInstance, "End4711");

        // assert that process ended not due to error boundary event 0815
        assertProcessDidNotEnd(processInstance, "End0815");

        // assert that the variables unchanged
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        assertEquals(3, variables.size());
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        assertAll(
                () -> assertEquals("foo", variablesAsMap.get("var1")),
                () -> assertEquals("bar", variablesAsMap.get("var2")),
                () -> assertEquals("foobar", variablesAsMap.get("var3"))
        );

        /*
         * test common exception
         */

        mockEndpoint.reset();

        final String FAILURE = "FAILURE";

        // variables returned but must not be set since task will not be
        // completed
        mockEndpoint.whenAnyExchangeReceived(exchange -> {
            throw new Exception(FAILURE);
        });

        // call route "direct:testRoute"
        final ProducerTemplate template2 = camelContext.createProducerTemplate();
        try {
            template2.requestBodyAndHeader("direct:firstTestRoute",
                    null,
                    "CamundaBpmExternalTaskId",
                    lockedExternalTask.getId());
            fail("Expected an exception, but Camel route succeeded!");
        } catch (Exception e) {
            // expected
        }

        // ensure endpoint has been called
        assertNotNull(mockEndpoint.assertExchangeReceived(0));

        // external task is still not resolved
        final List<ExternalTask> externalTasks4 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks4);
        assertEquals(1, externalTasks4.size());
        // Exception aborted processing so retries could not be set! Really?
        assertEquals(2, externalTasks4.get(0).getRetries());
        assertEquals(FAILURE, externalTasks4.get(0).getErrorMessage());

        // assert that process not in the end event "HappyEnd"
        assertProcessDidNotEnd(processInstance, "HappyEnd");

        // assert that process ended not due to error boundary event 4711
        assertProcessDidNotEnd(processInstance, "End4711");

        // assert that process ended not due to error boundary event 0815
        assertProcessDidNotEnd(processInstance, "End0815");

        // assert that the variables unchanged
        final List<HistoricVariableInstance> variables2 = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        assertEquals(3, variables2.size());
        final HashMap<String, Object> variablesAsMap2 = new HashMap<>();
        for (final HistoricVariableInstance variable : variables2) {
            variablesAsMap2.put(variable.getName(), variable.getValue());
        }
        assertAll(
                () -> assertEquals("foo", variablesAsMap2.get("var1")),
                () -> assertEquals("bar", variablesAsMap2.get("var2")),
                () -> assertEquals("foobar", variablesAsMap2.get("var3"))
        );

        // complete task to make test order not relevant
        externalTaskService.complete(externalTasks2.get(0).getId(), "0815");
    }

    @Test
    public void testSetExternalTaskRetriesAnnotation() {
        deployProcess("process/StartExternalTask4.bpmn20.xml");
        // start process
        final Map<String, Object> processVariables = new HashMap<>();
        processVariables.put("var1", "foo");
        processVariables.put("var2", "bar");
        processVariables.put("var3", "foobar");
        final ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("startExternalTaskProcess2",
                processVariables);
        assertNotNull(processInstance);

        // external task is still not resolved and not locked
        final List<ExternalTask> externalTasks1 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks1);
        assertEquals(1, externalTasks1.size());
        assertNull(externalTasks1.get(0).getWorkerId());
        assertNull(externalTasks1.get(0).getRetries());

        // find external task and lock
        final List<LockedExternalTask> locked = externalTaskService.fetchAndLock(1, "0815", true).topic("topic4", 5000).execute();
        assertNotNull(locked);
        assertEquals(1, locked.size());
        final LockedExternalTask lockedExternalTask = locked.get(0);

        // set retries artificially
        externalTaskService.handleFailure(lockedExternalTask.getId(), "0815", "Blabal", 2, 0);

        /*
         * DoNotChangeRetriesException
         */

        mockEndpoint.reset();

        final String DONTCHANGEMSG = "DoNotChange";

        // variables returned but must not be set since task will not be
        // completed
        mockEndpoint.whenAnyExchangeReceived(exchange -> {
            throw new DontChangeRetriesException(DONTCHANGEMSG);
        });

        // call route "direct:testRoute"
        final ProducerTemplate template2 = camelContext.createProducerTemplate();
        try {
            template2.requestBodyAndHeader("direct:firstTestRoute",
                    null,
                    "CamundaBpmExternalTaskId",
                    lockedExternalTask.getId());
            fail("Expected an exception, but Camel route succeeded!");
        } catch (Exception e) {
            // expected
        }

        // ensure endpoint has been called
        assertNotNull(mockEndpoint.assertExchangeReceived(0));

        // external task is still not resolved
        final List<ExternalTask> externalTasks4 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks4);
        assertEquals(1, externalTasks4.size());
        // Exception aborted processing so retries could not be set!
        assertEquals(2, externalTasks4.get(0).getRetries());
        assertEquals(DONTCHANGEMSG, externalTasks4.get(0).getErrorMessage());
        // assert that process not in the end event "HappyEnd"
        assertProcessDidNotEnd(processInstance, "HappyEnd");

        // assert that process ended not due to error boundary event 4711
        assertProcessDidNotEnd(processInstance, "End4711");

        // assert that process ended not due to error boundary event 0815
        assertProcessDidNotEnd(processInstance, "End0815");

        // assert that the variables unchanged
        final List<HistoricVariableInstance> variables2 = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        assertEquals(3, variables2.size());
        final HashMap<String, Object> variablesAsMap2 = new HashMap<>();
        for (final HistoricVariableInstance variable : variables2) {
            variablesAsMap2.put(variable.getName(), variable.getValue());
        }
        assertAll(
                () -> assertEquals("foo", variablesAsMap2.get("var1")),
                () -> assertEquals("bar", variablesAsMap2.get("var2")),
                () -> assertEquals("foobar", variablesAsMap2.get("var3"))
        );

        /*
         * DoNotChangeRetriesException
         */
        mockEndpoint.reset();
        final String CREATEINCIDENT = "Incident";

        // variables returned but must not be set since task will not be
        // completed
        mockEndpoint.whenAnyExchangeReceived(exchange -> {
            throw new CreateIncidentException(CREATEINCIDENT);
        });

        // call route "direct:testRoute"
        final ProducerTemplate template3 = camelContext.createProducerTemplate();
        try {
            template3.requestBodyAndHeader("direct:firstTestRoute",
                    null,
                    "CamundaBpmExternalTaskId",
                    lockedExternalTask.getId());
            fail("Expected an exception, but Camel route succeeded!");
        } catch (Exception e) {
            // expected
        }

        // ensure endpoint has been called
        assertNotNull(mockEndpoint.assertExchangeReceived(0));

        // external task is still not resolved
        final List<ExternalTask> externalTasks3 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks3);
        assertEquals(1, externalTasks3.size());
        // Exception aborted processing so retries could not be set!
        assertEquals(0, externalTasks3.get(0).getRetries());
        assertEquals(CREATEINCIDENT, externalTasks3.get(0).getErrorMessage());


        // assert that process not in the end event "HappyEnd"
        assertProcessDidNotEnd(processInstance, "HappyEnd");

        // assert that process ended not due to error boundary event 4711
        assertProcessDidNotEnd(processInstance, "End4711");

        // assert that process ended not due to error boundary event 0815
        assertProcessDidNotEnd(processInstance, "End0815");

        // assert that the variables unchanged
        final List<HistoricVariableInstance> variables3 = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        assertEquals(3, variables3.size());
        final HashMap<String, Object> variablesAsMap3 = new HashMap<>();
        for (final HistoricVariableInstance variable : variables3) {
            variablesAsMap3.put(variable.getName(), variable.getValue());
        }
        assertAll(
                () -> assertEquals("foo", variablesAsMap3.get("var1")),
                () -> assertEquals("bar", variablesAsMap3.get("var2")),
                () -> assertEquals("foobar", variablesAsMap3.get("var3"))
        );

        // complete task to make test order not relevant
        externalTaskService.complete(externalTasks1.get(0).getId(), "0815");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBpmnError() throws Exception {
        deployProcess("process/StartExternalTask.bpmn20.xml");
        // variables to be set by the Camel-endpoint processing the external
        // task
        mockEndpoint.returnReplyBody(new Expression() {
            @Override
            public <T> T evaluate(Exchange exchange, Class<T> type) {
                return (T) "4711";
            }
        });

        // start process
        final Map<String, Object> processVariables = new HashMap<>();
        processVariables.put("var1", "foo");
        processVariables.put("var2", "bar");
        final ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("startExternalTaskProcess",
                processVariables);
        assertNotNull(processInstance);

        // wait for the external task to be completed
        // external task is resolved
        await().atMost(1, TimeUnit.SECONDS).until(() -> externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).active().count() == 0);
//
        // assert that the camunda BPM process instance ID has been added as a
        // property to the message
        assertEquals(processInstance.getId(), mockEndpoint
                .assertExchangeReceived(0)
                .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID));
//
        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        assertEquals(2, variables.size());
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        assertEquals("foo", variablesAsMap.get("var1"));
        assertEquals("bar", variablesAsMap.get("var2"));

        // assert that process ended
        final HistoricProcessInstance historicProcessInstance = historyService.createHistoricProcessInstanceQuery().processInstanceId(
                processInstance.getId()).singleResult();
        assertNotNull(historicProcessInstance.getEndTime());

        // assert that process ended due to error boundary event 4711
        assertNotNull(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End4711")
                .singleResult());
        // assert that process not in end event "HappyEnd"
        assertProcessDidNotEnd(processInstance, "HappyEnd");

        // assert that process ended not due to error boundary event 0815
        assertProcessDidNotEnd(processInstance, "End0815");
    }

    @Test
    public void testIncidentAndRetryTimeouts() throws Exception {
        deployProcess("process/StartExternalTask.bpmn20.xml");

        // variables to be set by the Camel-endpoint processing the external
        // task
        mockEndpoint.whenAnyExchangeReceived(exchange -> {
            throw new RuntimeException("fail!");
        });

        // count incidents for later comparison
        final long incidentCount = runtimeService.createIncidentQuery().count();

        // start process
        final Map<String, Object> processVariables = new HashMap<>();
        processVariables.put("var1", "foo");
        processVariables.put("var2", "bar");
        final ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("startExternalTaskProcess",
                processVariables);
        assertNotNull(processInstance);

        // wait for the external task to be completed
        await().atMost(2, TimeUnit.SECONDS).until(() -> !externalTaskService.createExternalTaskQuery()
                .processInstanceId(processInstance.getId()).list().isEmpty()
                && externalTaskService.createExternalTaskQuery()
                .processInstanceId(processInstance.getId()).list().get(0).getRetries() != null
                && externalTaskService.createExternalTaskQuery()
                .processInstanceId(processInstance.getId()).list().get(0).getRetries() == 2);

        // external task is still not resolved
        final List<ExternalTask> externalTasks1 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks1);
        assertEquals(1, externalTasks1.size());
        assertEquals(2, externalTasks1.get(0).getRetries());

        // wait for the next try
        await().atMost(2, TimeUnit.SECONDS).until(() -> externalTaskService.createExternalTaskQuery()
                .processInstanceId(processInstance.getId()).list().get(0).getRetries() == 1);

        // external task is still not resolved
        final List<ExternalTask> externalTasks2 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks2);
        assertEquals(1, externalTasks2.size());
        assertEquals(1, externalTasks2.get(0).getRetries());

        // next try is 2 seconds so after 1 second nothing changes
        Thread.sleep(1000);

        // external task is still not resolved
        final List<ExternalTask> externalTasks3 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks3);
        assertEquals(1, externalTasks3.size());
        assertEquals(1, externalTasks3.get(0).getRetries());

        // wait for the next try
        await().atMost(5, TimeUnit.SECONDS).until(() -> externalTaskService.createExternalTaskQuery()
                .processInstanceId(processInstance.getId()).list().get(0).getRetries() == 0);

        // external task is still not resolved
        final List<ExternalTask> externalTasks4 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        assertNotNull(externalTasks4);
        assertEquals(1, externalTasks4.size());
        assertEquals(0, externalTasks4.get(0).getRetries());

        // assert that the camunda BPM process instance ID has been added as a
        // property to the message
        assertEquals(processInstance.getId(), mockEndpoint
                .assertExchangeReceived(0)
                .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID));
        assertEquals(0, mockEndpoint
                .assertExchangeReceived(0)
                .getIn().getHeader(CamundaBpmConstants.EXCHANGE_HEADER_ATTEMPTSSTARTED));
        assertEquals(2, mockEndpoint
                .assertExchangeReceived(0)
                .getIn().getHeader(CamundaBpmConstants.EXCHANGE_HEADER_RETRIESLEFT));
        assertEquals(1, mockEndpoint
                .assertExchangeReceived(1)
                .getIn().getHeader(CamundaBpmConstants.EXCHANGE_HEADER_ATTEMPTSSTARTED));
        assertEquals(1, mockEndpoint
                .assertExchangeReceived(1)
                .getIn().getHeader(CamundaBpmConstants.EXCHANGE_HEADER_RETRIESLEFT));

        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        assertEquals(2, variables.size());
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        assertEquals("foo", variablesAsMap.get("var1"));
        assertEquals("bar", variablesAsMap.get("var2"));

        // assert that process not ended
        final HistoricProcessInstance historicProcessInstance = historyService.createHistoricProcessInstanceQuery().processInstanceId(
                processInstance.getId()).singleResult();
        assertNull(historicProcessInstance.getEndTime());

        // assert that incident raised
        assertEquals(incidentCount + 1, runtimeService.createIncidentQuery().count());
    }

    @SetExternalTaskRetries(retries = 0)
    public static class CreateIncidentException extends Exception {
        @Serial
        private static final long serialVersionUID = 1L;

        public CreateIncidentException(final String message) {
            super(message);
        }
    }

    @SetExternalTaskRetries(retries = 0, relative = true)
    public static class DontChangeRetriesException extends Exception {
        @Serial
        private static final long serialVersionUID = 1L;

        public DontChangeRetriesException(final String message) {
            super(message);
        }
    }

    private void assertProcessDidNotEnd(ProcessInstance processInstance, String endActivityId) {
        assertNull(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId(endActivityId)
                .singleResult()
        );
    }

    private void assertHappyEnd(ProcessInstance processInstance) {
        // assert that process in end event "HappyEnd"
        assertNotNull(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("HappyEnd")
                .singleResult()
        );
    }

    @AfterEach
    void resetMock() {
        mockEndpoint.reset();
    }
}