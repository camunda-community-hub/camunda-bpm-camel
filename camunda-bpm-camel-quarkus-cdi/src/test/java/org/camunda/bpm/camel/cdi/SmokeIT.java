package org.camunda.bpm.camel.cdi;

import org.apache.camel.EndpointInject;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.camunda.bpm.engine.task.Task;
import org.camunda.bpm.quarkus.engine.extension.event.CamundaEngineStartupEvent;
import org.junit.jupiter.api.Test;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.h2.H2DatabaseTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/*
 * This test is basically a ... smoke test. It's a baseline to check that all the moving pieces 
 * (Maven deps, Arquillian config, ...) are working OK:
 *   - process definition deployment
 *   - Camel context instantiation and route startup
 *   - ...
 *   
 *   See other tests in this package for actual exercising of the camunda BPM <-> Camel integration
 */
@QuarkusTest
@QuarkusTestResource(H2DatabaseTestResource.class)
public class SmokeIT extends BaseQuarkusIntegrationTest {

  private static final String PROCESS_DEFINITION_KEY = "smokeTestProcess";

  @Inject
  @EndpointInject("mock:resultEndpoint3")
  MockEndpoint resultEndpoint;

  // Method is called as soon as the Process Engine is running
  public void deployProcess(@Observes CamundaEngineStartupEvent startupEvent) {
    deployProcess(  "process/SmokeTest.bpmn20.xml");
  }

  @Produces
  @ApplicationScoped
  public RouteBuilder createRoute() {
    return new RouteBuilder() {
      public void configure() {
        from("timer://smoke-message?repeatCount=1")
          .routeId("smoke-test-route")
          .to("log:org.camunda.bpm.camel.cdi?level=INFO&showAll=true&multiline=true")
          .to(resultEndpoint)
        ;
      }
    };
  }

  @Test
  public void doTest() throws InterruptedException {

    assertNotNull(camelContextBootstrap);
    assertNotNull(camelContextBootstrap.getCamelContext());
    assertNotNull(camelService);

    runtimeService.startProcessInstanceByKey(PROCESS_DEFINITION_KEY);
    Task task = taskService.createTaskQuery().processDefinitionKey(PROCESS_DEFINITION_KEY).singleResult();

    assertNotNull(task);
    assertEquals("My Task", task.getName());
    assertEquals(1, runtimeService.createProcessInstanceQuery().processDefinitionKey(PROCESS_DEFINITION_KEY).count());

    taskService.complete(task.getId());
    assertEquals(0, runtimeService.createProcessInstanceQuery().processDefinitionKey(PROCESS_DEFINITION_KEY).count());
    resultEndpoint.expectedMessageCount(1);
    resultEndpoint.assertIsSatisfied();
  }
}
