package org.camunda.bpm.camel.spring.util;

import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;

/**
 * ServiceTask to start with.
 * 
 * @author stefan.schulze@accelsis.biz
 *
 */
public class DummyJavaDelegate implements JavaDelegate {
	@Override
	public void execute(DelegateExecution execution) {
		// dummy
	}
}
