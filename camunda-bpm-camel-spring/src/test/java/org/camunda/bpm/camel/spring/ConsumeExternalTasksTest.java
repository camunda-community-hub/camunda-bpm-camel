package org.camunda.bpm.camel.spring;

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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Expression;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.mock.MockEndpoint;
import org.assertj.core.api.Assertions;
import org.camunda.bpm.camel.component.CamundaBpmConstants;
import org.camunda.bpm.camel.component.externaltasks.SetExternalTaskRetries;
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.externaltask.ExternalTask;
import org.camunda.bpm.engine.externaltask.LockedExternalTask;
import org.camunda.bpm.engine.history.HistoricProcessInstance;
import org.camunda.bpm.engine.history.HistoricVariableInstance;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.test.Deployment;
import org.camunda.bpm.engine.test.ProcessEngineRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:consume-external-tasks-config.xml")
public class ConsumeExternalTasksTest {
    @SetExternalTaskRetries(retries = 0)
    public static class CreateIncidentException extends Exception {
        private static final long serialVersionUID = 1L;

        public CreateIncidentException(final String message) {
            super(message);
        }
    }

    @SetExternalTaskRetries(retries = 0, relative = true)
    public static class DontChangeRetriesException extends Exception {
        private static final long serialVersionUID = 1L;

        public DontChangeRetriesException(final String message) {
            super(message);
        }
    }

    private MockEndpoint mockEndpoint;

    @Autowired
    private CamelContext camelContext;

    @Autowired
    private RuntimeService runtimeService;

    @Autowired
    private HistoryService historyService;

    @Autowired
    private ExternalTaskService externalTaskService;

    @Autowired
    @Rule
    public ProcessEngineRule processEngineRule;

    @Before
    public void setUp() {
        mockEndpoint = (MockEndpoint) camelContext.getEndpoint("mock:endpoint");
        mockEndpoint.reset();
    }

    @Test
    @Deployment(resources = { "process/StartExternalTask.bpmn20.xml" })
    @SuppressWarnings("unchecked")
    public void testSetProcessVariables() throws Exception {
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
        Assertions.assertThat(processInstance).isNotNull();

        // wait for the external task to be completed
        Thread.sleep(1000);

        // external task is not open any more
        final long externalTasksCount = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).active().count();
        Assertions.assertThat(externalTasksCount).isZero();

        // assert that the camunda BPM process instance ID has been added as a
        // property to the message
        Assertions
            .assertThat(mockEndpoint
                .assertExchangeReceived(0)
                .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID))
            .isEqualTo(processInstance.getId());

        // all process instance variables are loaded since no "variablesToFetch"
        // parameter was given
        var exchangeIn = mockEndpoint.assertExchangeReceived(0).getIn();
        Assertions.assertThat(exchangeIn.getBody()).isNotNull();
        Assertions.assertThat(exchangeIn.getBody()).isInstanceOf(Map.class);
        Assertions.assertThat(exchangeIn.getBody(Map.class)).hasSize(2);
        Assertions.assertThat(exchangeIn.getBody(Map.class)).containsEntry("var1", "foo");
        Assertions.assertThat(exchangeIn.getBody(Map.class)).containsEntry("var2", "bar");

        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(variables).hasSize(3);
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        Assertions.assertThat(variablesAsMap)
            .containsEntry("var1", "foo")
            .containsEntry("var2", "barbar")
            .containsEntry("var3", "bar3");

        // assert that process in end event "HappyEnd"
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("HappyEnd")
                .singleResult())
            .isNotNull();

        // assert that process ended not due to error boundary event 4711
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End4711")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 0815
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End0815")
                .singleResult())
            .isNull();
    }

    @Test
    @Deployment(resources = { "process/StartExternalTask3.bpmn20.xml" })
    @SuppressWarnings("unchecked")
    public void testLoadNoProcessVariablesAndAsyncIsFalse() throws Exception {
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
        Assertions.assertThat(processInstance).isNotNull();

        // wait for the external task to be completed
        Thread.sleep(1000);

        // external task is not open any more
        final long externalTasksCount = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).active().count();
        Assertions.assertThat(externalTasksCount).isZero();

        // assert that the camunda BPM process instance ID has been added as a
        // property to the message
        Assertions
            .assertThat(mockEndpoint
                .assertExchangeReceived(0)
                .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID))
            .isEqualTo(processInstance.getId());

        // no process instance variables are loaded since an empty
        // "variablesToFetch" parameter was given
        Assertions.assertThat(mockEndpoint.assertExchangeReceived(0).getIn().getBody())
            .isNotNull()
            .isInstanceOf(Map.class);
        Assertions
            .assertThat(mockEndpoint
                .assertExchangeReceived(0)
                .getIn()
                .getBody(Map.class))
            .isEmpty();

        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(variables).hasSize(2);
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        Assertions.assertThat(variablesAsMap)
            .containsEntry("var1", "foo1")
            .containsEntry("var2", "bar2");

        // assert that process in end event "HappyEnd"
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("HappyEnd")
                .singleResult())
            .isNotNull();

        // assert that process ended not due to error boundary event 4711
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End4711")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 0815
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End0815")
                .singleResult())
            .isNull();
    }

    @Test
    @Deployment(resources = { "process/StartExternalTask4.bpmn20.xml" })
    public void testCompleteTaskOnCompletionSuccessfully() {
        // variables returned but must not be set since task will not be
        // completed
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
        Assertions.assertThat(processInstance).isNotNull();

        // external task is still not resolved and not locked
        final List<ExternalTask> externalTasks1 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks1)
            .isNotNull()
            .hasSize(1);
        Assertions.assertThat(externalTasks1.get(0).getWorkerId()).isNull();

        // find external task and lock
        final List<LockedExternalTask> locked = externalTaskService.fetchAndLock(1, "0815", true).topic("topic4",
                5000).execute();
        Assertions.assertThat(locked)
            .isNotNull()
            .hasSize(1);
        final LockedExternalTask lockedExternalTask = locked.get(0);

        // call route "direct:testRoute"
        final ProducerTemplate template = camelContext.createProducerTemplate();
        template.requestBodyAndHeader("direct:firstTestRoute",
                null,
                "CamundaBpmExternalTaskId",
                lockedExternalTask.getId());

        // ensure endpoint has been called
        Assertions.assertThat(mockEndpoint.assertExchangeReceived(0)).isNotNull();
        Assertions
            .assertThat(mockEndpoint
                .assertExchangeReceived(0)
                .getIn()
                .getHeader(CamundaBpmConstants.EXCHANGE_HEADER_ATTEMPTSSTARTED))
            .isEqualTo(0);
        Assertions
            .assertThat(mockEndpoint
                .assertExchangeReceived(0)
                .getIn()
                .getHeader(CamundaBpmConstants.EXCHANGE_HEADER_RETRIESLEFT))
            .isEqualTo(2);

        // assert that process in end event "HappyEnd"
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("HappyEnd")
                .singleResult())
            .isNotNull();

        // assert that process ended not due to error boundary event 4711
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End4711")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 0815
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End0815")
                .singleResult())
            .isNull();

        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(variables).hasSize(3);
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        Assertions.assertThat(variablesAsMap)
            .containsEntry("var1", "foo")
            .containsEntry("var2", "bar2")
            .containsEntry("var3", "bar3");
    }

    @Test
    @Deployment(resources = { "process/StartExternalTask4.bpmn20.xml" })
    public void testCompleteTaskOnCompletionFailure() {
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
        Assertions.assertThat(processInstance).isNotNull();

        // external task is still not resolved and not locked
        final List<ExternalTask> externalTasks1 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks1)
            .isNotNull()
            .hasSize(1);
        Assertions.assertThat(externalTasks1.get(0).getWorkerId()).isNull();
        Assertions.assertThat(externalTasks1.get(0).getRetries()).isNull();

        // find external task and lock
        final List<LockedExternalTask> locked = externalTaskService.fetchAndLock(1, "0815", true).topic("topic4",
                5000).execute();
        Assertions.assertThat(locked)
            .isNotNull()
            .hasSize(1);
        final LockedExternalTask lockedExternalTask = locked.get(0);

        // call route "direct:testRoute"
        final ProducerTemplate template = camelContext.createProducerTemplate();
        try {
            template.requestBodyAndHeader("direct:firstTestRoute",
                    null,
                    "CamundaBpmExternalTaskId",
                    lockedExternalTask.getId());
            Assert.fail("Expected an exception, but Camel route succeeded!");
        } catch (Exception e) {
            // expected
        }

        // ensure endpoint has been called
        Assertions.assertThat(mockEndpoint.assertExchangeReceived(0)).isNotNull();

        // external task is still not resolved
        final List<ExternalTask> externalTasks2 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks2)
            .isNotNull()
            .hasSize(1);
        Assertions.assertThat(externalTasks2.get(0).getRetries()).isEqualTo(2);

        // assert that process not in the end event "HappyEnd"
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("HappyEnd")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 4711
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End4711")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 0815
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End0815")
                .singleResult())
            .isNull();

        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(variables).hasSize(3);
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        Assertions.assertThat(variablesAsMap)
            .containsEntry("var1", "foo")
            .containsEntry("var2", "bar")
            .containsEntry("var3", "foobar");

        // complete task to make test order not relevant
        externalTaskService.complete(externalTasks2.get(0).getId(), "0815");
    }

    @SuppressWarnings("unchecked")
    @Test
    @Deployment(resources = { "process/StartExternalTask4.bpmn20.xml" })
    public void testCompleteTaskSuccessfully() {
        // variables returned but must not be set since task will not be
        // completed
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
        Assertions.assertThat(processInstance).isNotNull();

        // external task is still not resolved and not locked
        final List<ExternalTask> externalTasks1 = externalTaskService
            .createExternalTaskQuery()
            .processInstanceId(processInstance.getId())
            .list();
        Assertions.assertThat(externalTasks1)
            .isNotNull()
            .hasSize(1);
        Assertions.assertThat(externalTasks1.get(0).getWorkerId()).isNull();

        // find external task and lock
        final List<LockedExternalTask> locked = externalTaskService.fetchAndLock(1, "0815", true).topic("topic4",
                5000).execute();
        Assertions.assertThat(locked)
            .isNotNull()
            .hasSize(1);
        final LockedExternalTask lockedExternalTask = locked.get(0);

        // call route "direct:testRoute"
        final ProducerTemplate template = camelContext.createProducerTemplate();
        template.requestBodyAndHeader("direct:secondTestRoute",
                null,
                "CamundaBpmExternalTaskId",
                lockedExternalTask.getId());

        // ensure endpoint has been called
        Assertions.assertThat(mockEndpoint.assertExchangeReceived(0)).isNotNull();

        // assert that process in end event "HappyEnd"
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("HappyEnd")
                .singleResult())
            .isNotNull();

        // assert that process ended not due to error boundary event 4711
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End4711")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 0815
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End0815")
                .singleResult())
            .isNull();

        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(variables).hasSize(3);
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        Assertions.assertThat(variablesAsMap)
            .containsEntry("var1", "foo")
            .containsEntry("var2", "bar2")
            .containsEntry("var3", "bar3");
    }

    @Test
    @Deployment(resources = { "process/StartExternalTask4.bpmn20.xml" })
    public void testCompleteTaskFailure() {
        // start process
        final Map<String, Object> processVariables = new HashMap<>();
        processVariables.put("var1", "foo");
        processVariables.put("var2", "bar");
        processVariables.put("var3", "foobar");
        final ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("startExternalTaskProcess2",
                processVariables);
        Assertions.assertThat(processInstance).isNotNull();

        // external task is still not resolved and not locked
        final List<ExternalTask> externalTasks1 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks1)
            .isNotNull()
            .hasSize(1);
        Assertions.assertThat(externalTasks1.get(0).getWorkerId()).isNull();
        Assertions.assertThat(externalTasks1.get(0).getRetries()).isNull();

        // find external task and lock
        final List<LockedExternalTask> locked = externalTaskService.fetchAndLock(1, "0815", true).topic("topic4",
                5000).execute();
        Assertions.assertThat(locked)
            .isNotNull()
            .hasSize(1);
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
        Assertions.assertThat(mockEndpoint.assertExchangeReceived(0)).isNotNull();

        // external task is still not resolved
        final List<ExternalTask> externalTasks2 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks2)
            .isNotNull()
            .hasSize(1);
        // Exception aborted processing so retries could not be set!
        Assertions.assertThat(externalTasks2.get(0).getRetries()).isNull();
        Assertions.assertThat(externalTasks2.get(0).getErrorMessage()).isNull();

        // assert that process not in the end event "HappyEnd"
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("HappyEnd")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 4711
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End4711")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 0815
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End0815")
                .singleResult())
            .isNull();

        // assert that the variables unchanged
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(variables).hasSize(3);
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        Assertions.assertThat(variablesAsMap)
            .containsEntry("var1", "foo")
            .containsEntry("var2", "bar")
            .containsEntry("var3", "foobar");

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
            Assert.fail("Expected an exception, but Camel route succeeded!");
        } catch (Exception e) {
            // expected
        }

        // ensure endpoint has been called
        Assertions.assertThat(mockEndpoint.assertExchangeReceived(0)).isNotNull();

        // external task is still not resolved
        final List<ExternalTask> externalTasks4 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks4)
            .isNotNull()
            .hasSize(1);
        // Exception aborted processing so retries could not be set!
        Assertions.assertThat(externalTasks4.get(0).getRetries()).isEqualTo(2);
        Assertions.assertThat(externalTasks4.get(0).getErrorMessage()).isEqualTo(FAILURE);

        // assert that process not in the end event "HappyEnd"
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("HappyEnd")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 4711
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End4711")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 0815
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End0815")
                .singleResult())
            .isNull();

        // assert that the variables unchanged
        final List<HistoricVariableInstance> variables2 = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(variables2).hasSize(3);
        final HashMap<String, Object> variablesAsMap2 = new HashMap<>();
        for (final HistoricVariableInstance variable : variables2) {
            variablesAsMap2.put(variable.getName(), variable.getValue());
        }
        Assertions.assertThat(variablesAsMap2)
            .containsEntry("var1", "foo")
            .containsEntry("var2", "bar")
            .containsEntry("var3", "foobar");

        // complete task to make test order not relevant
        externalTaskService.complete(externalTasks2.get(0).getId(), "0815");

    }

    @Test
    @Deployment(resources = { "process/StartExternalTask4.bpmn20.xml" })
    public void testSetExternalTaskRetriesAnnotation() {
        // start process
        final Map<String, Object> processVariables = new HashMap<>();
        processVariables.put("var1", "foo");
        processVariables.put("var2", "bar");
        processVariables.put("var3", "foobar");
        final ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("startExternalTaskProcess2",
                processVariables);
        Assertions.assertThat(processInstance).isNotNull();

        // external task is still not resolved and not locked
        final List<ExternalTask> externalTasks1 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks1)
            .isNotNull()
            .hasSize(1);
        Assertions.assertThat(externalTasks1.get(0).getWorkerId()).isNull();
        Assertions.assertThat(externalTasks1.get(0).getRetries()).isNull();

        // find external task and lock
        final List<LockedExternalTask> locked = externalTaskService.fetchAndLock(1, "0815", true).topic("topic4",
                5000).execute();
        Assertions.assertThat(locked)
            .isNotNull()
            .hasSize(1);
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
            Assert.fail("Expected an exception, but Camel route succeeded!");
        } catch (Exception e) {
            // expected
        }

        // ensure endpoint has been called
        Assertions.assertThat(mockEndpoint.assertExchangeReceived(0)).isNotNull();

        // external task is still not resolved
        final List<ExternalTask> externalTasks4 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks4)
            .isNotNull()
            .hasSize(1);
        // Exception aborted processing so retries could not be set!
        Assertions.assertThat(externalTasks4.get(0).getRetries()).isEqualTo(2);
        Assertions.assertThat(externalTasks4.get(0).getErrorMessage()).isEqualTo(DONTCHANGEMSG);

        // assert that process not in the end event "HappyEnd"
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("HappyEnd")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 4711
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End4711")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 0815
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End0815")
                .singleResult())
            .isNull();

        // assert that the variables unchanged
        final List<HistoricVariableInstance> variables2 = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(variables2).hasSize(3);
        final HashMap<String, Object> variablesAsMap2 = new HashMap<>();
        for (final HistoricVariableInstance variable : variables2) {
            variablesAsMap2.put(variable.getName(), variable.getValue());
        }
        Assertions.assertThat(variablesAsMap2)
            .containsEntry("var1", "foo")
            .containsEntry("var2", "bar")
            .containsEntry("var3", "foobar");
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
            Assert.fail("Expected an exception, but Camel route succeeded!");
        } catch (Exception e) {
            // expected
        }

        // ensure endpoint has been called
        Assertions.assertThat(mockEndpoint.assertExchangeReceived(0)).isNotNull();

        // external task is still not resolved
        final List<ExternalTask> externalTasks3 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks3)
            .isNotNull()
            .hasSize(1);
        // Exception aborted processing so retries could not be set!
        Assertions.assertThat(externalTasks3.get(0).getRetries()).isZero();
        Assertions.assertThat(externalTasks3.get(0).getErrorMessage()).isEqualTo(CREATEINCIDENT);

        // assert that process not in the end event "HappyEnd"
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("HappyEnd")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 4711
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End4711")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 0815
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End0815")
                .singleResult())
            .isNull();

        // assert that the variables unchanged
        final List<HistoricVariableInstance> variables3 = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(variables3).hasSize(3);
        final HashMap<String, Object> variablesAsMap3 = new HashMap<>();
        for (final HistoricVariableInstance variable : variables3) {
            variablesAsMap3.put(variable.getName(), variable.getValue());
        }
        Assertions.assertThat(variablesAsMap3)
            .containsEntry("var1", "foo")
            .containsEntry("var2", "bar")
            .containsEntry("var3", "foobar");
        // complete task to make test order not relevant
        externalTaskService.complete(externalTasks1.get(0).getId(), "0815");
    }

    @Test
    @Deployment(resources = { "process/StartExternalTask2.bpmn20.xml" })
    @SuppressWarnings("unchecked")
    public void testAsyncIsTrueAndLockDuration() throws Exception {
        // variables returned but must not be set since task will not be
        // completed
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
        Assertions.assertThat(processInstance).isNotNull();

        // wait for the external task to be completed
        Thread.sleep(1000);

        // external task is still not resolved and locked
        final List<ExternalTask> externalTasks1 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks1)
            .isNotNull()
            .hasSize(1);
        Assertions.assertThat(externalTasks1.get(0).getWorkerId()).isEqualTo("0815");
        Assertions.assertThat(externalTasks1.get(0).getLockExpirationTime()).isAfter(new Date());

        // wait for the task to unlock and re-fetch by Camel
        Thread.sleep(2000);

        // assert that the camunda BPM process instance ID has been added as a
        // property to the message for both exchanges received
        Assertions
            .assertThat(mockEndpoint
                .assertExchangeReceived(0)
                .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID))
            .isEqualTo(processInstance.getId());
        Assertions
            .assertThat(mockEndpoint
                .assertExchangeReceived(1)
                .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID))
            .isEqualTo(processInstance.getId());

        // only two process instance variables are loaded according configured
        // value of "variablesToFetch" parameter
        Assertions.assertThat(mockEndpoint.assertExchangeReceived(0).getIn().getBody()).isNotNull();
        Assertions.assertThat(mockEndpoint.assertExchangeReceived(0).getIn().getBody()).isInstanceOf(Map.class);
        Assertions.assertThat(mockEndpoint.assertExchangeReceived(0).getIn().getBody(Map.class)).hasSize(2);
        Assertions.assertThat(mockEndpoint.assertExchangeReceived(0).getIn().getBody(Map.class))
            .containsEntry("var2", "bar")
            .containsEntry("var3", "foobar");

        // assert that the variables sent in the response-message has NOT been
        // set into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(variables).hasSize(3);
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        Assertions.assertThat(variablesAsMap)
            .containsEntry("var1", "foo")
            .containsEntry("var2", "bar")
            .containsEntry("var3", "foobar");

        // assert that process NOT in end event "HappyEnd"
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("HappyEnd")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 4711
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End4711")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 0815
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End0815")
                .singleResult())
            .isNull();

    }

    @SuppressWarnings("unchecked")
    @Test
    @Deployment(resources = { "process/StartExternalTask.bpmn20.xml" })
    public void testBpmnError() throws Exception {
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
        Assertions.assertThat(processInstance).isNotNull();

        // wait for the external task to be completed
        Thread.sleep(1000);

        // external task is still not resolved
        final long externalTasksCount = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).active().count();
        Assertions.assertThat(externalTasksCount).isZero();

        // assert that the camunda BPM process instance ID has been added as a
        // property to the message
        Assertions
            .assertThat(mockEndpoint
                .assertExchangeReceived(0)
                .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID))
            .isEqualTo(processInstance.getId());

        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(variables).hasSize(2);
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        Assertions.assertThat(variablesAsMap)
            .containsEntry("var1", "foo")
            .containsEntry("var2", "bar");

        // assert that process ended
        final HistoricProcessInstance historicProcessInstance = historyService.createHistoricProcessInstanceQuery().processInstanceId(
                processInstance.getId()).singleResult();
        Assertions.assertThat(historicProcessInstance.getEndTime()).isNotNull();

        // assert that process ended due to error boundary event 4711
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End4711")
                .singleResult())
            .isNotNull();

        // assert that process not in end event "HappyEnd"
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("HappyEnd")
                .singleResult())
            .isNull();

        // assert that process ended not due to error boundary event 0815
        Assertions
            .assertThat(historyService
                .createHistoricActivityInstanceQuery()
                .processInstanceId(processInstance.getId())
                .activityId("End0815")
                .singleResult())
            .isNull();
    }

    @Test
    @Deployment(resources = { "process/StartExternalTask.bpmn20.xml" })
    public void testIncidentAndRetryTimeouts() throws Exception {
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
        Assertions.assertThat(processInstance).isNotNull();

        // wait for the external task to be completed
        Thread.sleep(1000);

        // external task is still not resolved
        final List<ExternalTask> externalTasks1 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks1)
            .isNotNull()
            .hasSize(1);
        Assertions.assertThat(externalTasks1.get(0).getRetries()).isEqualTo(2);

        // wait for the next try
        Thread.sleep(1000);

        // external task is still not resolved
        final List<ExternalTask> externalTasks2 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks2)
            .isNotNull()
            .hasSize(1);
        Assertions.assertThat(externalTasks2.get(0).getRetries()).isEqualTo(1);

        // next try is 2 seconds so after 1 second nothing changes
        Thread.sleep(1000);

        // external task is still not resolved
        final List<ExternalTask> externalTasks3 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks3)
            .isNotNull()
            .hasSize(1);
        Assertions.assertThat(externalTasks3.get(0).getRetries()).isEqualTo(1);

        // wait for the next try
        Thread.sleep(1000);

        // external task is still not resolved
        final List<ExternalTask> externalTasks4 = externalTaskService.createExternalTaskQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(externalTasks4)
            .isNotNull()
            .hasSize(1);
        Assertions.assertThat(externalTasks4.get(0).getRetries()).isZero();

        // assert that the camunda BPM process instance ID has been added as a
        // property to the message
        Assertions
            .assertThat(mockEndpoint
                .assertExchangeReceived(0)
                .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID))
            .isEqualTo(processInstance.getId());
        // assert that in-headers are set properly
        Assertions
            .assertThat(mockEndpoint
                .assertExchangeReceived(0)
                .getIn()
                .getHeader(CamundaBpmConstants.EXCHANGE_HEADER_ATTEMPTSSTARTED))
            .isEqualTo(0);
        Assertions
            .assertThat(mockEndpoint
                .assertExchangeReceived(0)
                .getIn()
                .getHeader(CamundaBpmConstants.EXCHANGE_HEADER_RETRIESLEFT))
            .isEqualTo(2);
        Assertions
            .assertThat(mockEndpoint
                .assertExchangeReceived(1)
                .getIn()
                .getHeader(CamundaBpmConstants.EXCHANGE_HEADER_ATTEMPTSSTARTED))
            .isEqualTo(1);
        Assertions
            .assertThat(mockEndpoint
                .assertExchangeReceived(1)
                .getIn()
                .getHeader(CamundaBpmConstants.EXCHANGE_HEADER_RETRIESLEFT))
            .isEqualTo(1);

        // assert that the variables sent in the response-message has been set
        // into the process
        final List<HistoricVariableInstance> variables = historyService.createHistoricVariableInstanceQuery().processInstanceId(
                processInstance.getId()).list();
        Assertions.assertThat(variables).hasSize(2);
        final HashMap<String, Object> variablesAsMap = new HashMap<>();
        for (final HistoricVariableInstance variable : variables) {
            variablesAsMap.put(variable.getName(), variable.getValue());
        }
        Assertions.assertThat(variablesAsMap)
            .containsEntry("var1", "foo")
            .containsEntry("var2", "bar");

        // assert that process not ended
        final HistoricProcessInstance historicProcessInstance = historyService.createHistoricProcessInstanceQuery().processInstanceId(
                processInstance.getId()).singleResult();
        Assertions.assertThat(historicProcessInstance.getEndTime()).isNull();

        // assert that incident raised
        Assertions.assertThat(runtimeService.createIncidentQuery().count()).isEqualTo(incidentCount + 1);
    }
}
