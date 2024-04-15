package org.camunda.bpm.camel.cdi;

import java.util.HashMap;
import java.util.Map;
import org.apache.camel.EndpointInject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.quarkus.engine.extension.event.CamundaEngineStartupEvent;
import org.junit.jupiter.api.Test;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.h2.H2DatabaseTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;


@QuarkusTest
@QuarkusTestResource(H2DatabaseTestResource.class)
public class SendToCamelIT extends BaseQuarkusIntegrationTest {

    private static final String PROCESS_DEFINITION_KEY = "sendToCamelProcess";
    @Inject
    @EndpointInject("mock:resultEndpoint2")
    MockEndpoint resultEndpoint;

    // Method is called as soon as the Process Engine is running
    public void deployProcess(@Observes CamundaEngineStartupEvent startupEvent) {
        deployProcess("process/SendToCamelQuarkusDelegate.bpmn20.bpmn");
    }

    @Produces
    @ApplicationScoped
    public RouteBuilder createRoute() {
        return new RouteBuilder() {
            public void configure() {
                from("direct:sendToCamelServiceTask")
                        .routeId("send-to-camel-route")
                        .to("log:org.camunda.bpm.camel.cdi?level=INFO&showAll=true&multiline=true")
                        .to(resultEndpoint)
                ;
            }
        };
    }

    @Test
    public void doTest() {
        Map<String, Object> processVariables = new HashMap<String, Object>();
        processVariables.put("var1", "foo");
        processVariables.put("var2", "bar");
        processVariables.put("log", log);
        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("sendToCamelProcess", processVariables);

        // Verify that a process instance was executed and there are no instances executing now
        assertEquals(1, historyService.createHistoricProcessInstanceQuery().processDefinitionKey("sendToCamelProcess").count());
        assertEquals(0, runtimeService.createProcessInstanceQuery().processDefinitionKey("sendToCamelProcess").count());

        // Assert that the camunda BPM process instance ID has been added as a property to the message
        assertEquals(processInstance.getId(), resultEndpoint.assertExchangeReceived(0).getProperty(EXCHANGE_HEADER_PROCESS_INSTANCE_ID));

        // Assert that the body of the message received by the endpoint contains a hash map with the value of the process variable 'var1' sent from camunda BPM
        assertEquals("{var1=foo}", resultEndpoint.assertExchangeReceived(0).getIn().getBody(String.class));

        // FIXME: check that var2 is also present as a property!
    }
}