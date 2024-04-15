package org.camunda.bpm.camel.spring.util;

import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;

public class TestJoinDelegate implements JavaDelegate {
  @Override
  public void execute(DelegateExecution execution) throws Exception {
    // dummy task
  }
}
