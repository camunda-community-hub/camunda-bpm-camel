package org.camunda.bpm.camel.spring.util;

import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import static org.camunda.bpm.camel.component.CamundaBpmConstants.*;

public class InitDelegate implements JavaDelegate {
  @Override
  public void execute(DelegateExecution execution) throws Exception {
    execution.setVariable(EXCHANGE_HEADER_PROCESS_INSTANCE_ID, execution.getProcessInstanceId());
  }
}
