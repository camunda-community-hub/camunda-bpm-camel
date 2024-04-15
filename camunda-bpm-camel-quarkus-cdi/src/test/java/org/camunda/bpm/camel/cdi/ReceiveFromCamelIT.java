package org.camunda.bpm.camel.cdi;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.camel.EndpointInject;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.camunda.bpm.camel.cdi.quarkus.LogServiceCdiImpl;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.quarkus.engine.extension.event.CamundaEngineStartupEvent;
import org.junit.jupiter.api.Test;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.h2.H2DatabaseTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;


@QuarkusTest
@QuarkusTestResource(H2DatabaseTestResource.class)
public class ReceiveFromCamelIT extends BaseQuarkusIntegrationTest {

    private final static String PROCESS_DEFINITION_KEY = "receiveFromCamelProcess";

    @Inject
    @EndpointInject("mock:resultEndpoint")
    MockEndpoint resultEndpoint;
    @Inject
    @Identifier("cdiLog")
    LogServiceCdiImpl log;

    // Method is called as soon as the Process Engine is running
    public void deployProcess(@Observes CamundaEngineStartupEvent startupEvent) {
        deployProcess("process/ReceiveFromCamelQuarkus.bpmn20.xml");
    }

    @Produces
    @ApplicationScoped
    public RouteBuilder createRoute() {
        return new RouteBuilder() {
            public void configure() {
                from("direct:sendToCamundaBpm")
                        .routeId("receive-from-camel-route")
                        .to("log:org.camunda.bpm.camel.cdi?level=INFO&showAll=true&multiline=true")
                        .to("camunda-bpm://signal?processDefinitionKey=receiveFromCamelProcess&activityId=waitForCamel")
                        .to("log:org.camunda.bpm.camel.cdi?level=INFO&showAll=true&multiline=true")
                        .to(resultEndpoint)
                ;
            }
        };
    }

    @Test
    public void doTest() throws IOException {
        Map<String, Object> processVariables = new HashMap<>();
        processVariables.put("var1", "foo");
        processVariables.put("var2", "bar");
        processVariables.put("log", log);
//        processVariables.put("log", Variables.objectValue(log, true).serializationDataFormat(Variables.SerializationDataFormats.JAVA).create());
        ProcessInstance processInstance = runtimeService.startProcessInstanceByKey(PROCESS_DEFINITION_KEY, processVariables);

        // Verify that a process instance has executed and there is one instance executing now
        assertEquals(1, historyService.createHistoricProcessInstanceQuery().processDefinitionKey(PROCESS_DEFINITION_KEY).count());
        assertEquals(1, runtimeService.createProcessInstanceQuery().processDefinitionKey(PROCESS_DEFINITION_KEY).count());

        /*
         * We need the process instance ID to be able to send the message to it
         *
         * FIXME: we need to fix this with the process execution id or even better with the Activity Instance Model
         * http://camundabpm.blogspot.de/2013/06/introducing-activity-instance-model-to.html
         */
        try (ProducerTemplate tpl = camelContextBootstrap.getCamelContext().createProducerTemplate()) {
            tpl.sendBodyAndProperty("direct:sendToCamundaBpm", null, EXCHANGE_HEADER_PROCESS_INSTANCE_ID, processInstance.getId());
        }

        // Assert that the camunda BPM process instance ID has been added as a property to the message
        assertEquals(processInstance.getId(), resultEndpoint.assertExchangeReceived(0).getProperty(EXCHANGE_HEADER_PROCESS_INSTANCE_ID));

        // Assert that the process instance is finished
        assertEquals(0, runtimeService.createProcessInstanceQuery().processDefinitionKey(PROCESS_DEFINITION_KEY).count());
    }
}