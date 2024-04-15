package org.camunda.bpm.camel.spring.util;

import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.ExecutionListener;

/**
 * Attach to the ReceiveTask (end event).
 *
 * @author stefan.schulze@accelsis.biz
 */
public class DummyExecutionListener implements ExecutionListener {
  @Override
  public void notify(DelegateExecution execution) {
    // dummy
  }
}
