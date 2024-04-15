package org.camunda.bpm.camel.cdi;

import java.io.IOException;
import java.util.Map;
import org.apache.camel.EndpointInject;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
@QuarkusTestResource(H2DatabaseTestResource.class)
public class StartProcessFromRouteIT extends BaseQuarkusIntegrationTest {

    private static final String PROCESS_DEFINITION_KEY = "startProcessFromRoute";

    @Inject
    @EndpointInject("mock:mockEndpoint")
    MockEndpoint mockEndpoint;
    @Inject
    @EndpointInject("mock:processVariableEndpoint")
    MockEndpoint processVariableEndpoint;

    // Method is called as soon as the Process Engine is running
    public void deployProcess(@Observes CamundaEngineStartupEvent startupEvent) {
        deployProcess("process/StartProcessFromRouteQuarkus.bpmn20.xml");
    }

    @Produces
    @ApplicationScoped
    public RouteBuilder createRoute() {
        return new RouteBuilder() {
            public void configure() {
                from("direct:start")
                        .routeId("start-process-from-route")
                        .to("log:org.camunda.bpm.camel.cdi?level=INFO&showAll=true&multiline=true")
                        .to("camunda-bpm://start?processDefinitionKey=startProcessFromRoute&copyBodyAsVariable=var1")
                        .to("log:org.camunda.bpm.camel.cdi?level=INFO&showAll=true&multiline=true")
                        .to(mockEndpoint)
                ;

                from("direct:processVariable")
                        .routeId("processVariableRoute")
                        .to("log:org.camunda.bpm.camel.cdi?level=INFO&showAll=true&multiline=true")
                        .to(processVariableEndpoint)
                ;
            }
        };
    }

    @Test
    public void doTest() throws IOException {
        String processInstanceId;
        try (ProducerTemplate tpl = camelContextBootstrap.getCamelContext().createProducerTemplate()) {
            processInstanceId = (String) tpl.requestBody("direct:start", Map.of("var1", "valueOfVar1", "log", log));
        }
        assertNotNull(processInstanceId);
        System.out.println("Process instance ID: " + processInstanceId);

        // Verify that a process instance was executed and there are no instances executing now
        assertEquals(1, historyService.createHistoricProcessInstanceQuery().processDefinitionKey(PROCESS_DEFINITION_KEY).count());
        assertEquals(0, runtimeService.createProcessInstanceQuery().processDefinitionKey(PROCESS_DEFINITION_KEY).count());

        // Assert that the camunda BPM process instance ID has been added as a property to the message
        assertEquals(processInstanceId, mockEndpoint.assertExchangeReceived(0).getProperty(EXCHANGE_HEADER_PROCESS_INSTANCE_ID));

        // The body of the message coming out from the camunda-bpm:<process definition> endpoint is the process instance
        assertEquals(processInstanceId, mockEndpoint.assertExchangeReceived(0).getIn().getBody(String.class));

        // We should receive a hash map with the value of 'var1' as the body of the message
        assertEquals("{var1=valueOfVar1}", processVariableEndpoint.assertExchangeReceived(0).getIn().getBody(String.class));
    }
}