package org.camunda.bpm.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import org.camunda.bpm.camel.component.CamundaBpmComponent;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.RuntimeService;
import org.junit.jupiter.api.BeforeEach;

import static org.camunda.bpm.camel.component.CamundaBpmConstants.CAMUNDA_BPM_CAMEL_URI_SCHEME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BaseCamelTest {

  protected CamelContext camelContext = new DefaultCamelContext();
  protected ProcessEngine processEngine = mock(ProcessEngine.class);
  protected RuntimeService runtimeService = mock(RuntimeService.class);

  @BeforeEach
  void setUpMocksAndCamundaBpmComponent() {
    when(processEngine.getRuntimeService()).thenReturn(runtimeService);

    CamundaBpmComponent component = new CamundaBpmComponent(processEngine);
    camelContext.addComponent(CAMUNDA_BPM_CAMEL_URI_SCHEME, component);
  }

}
