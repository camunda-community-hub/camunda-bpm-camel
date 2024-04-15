package org.camunda.bpm.camel.cdi;

import java.io.IOException;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.camunda.bpm.camel.component.CamundaBpmConstants;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;

@Named
@Dependent
public class ServiceDelegateBean implements JavaDelegate {


    @Inject
    CamelContext camelCtx;

    @Override
    public void execute(DelegateExecution execution) {
//        String expression = (String) execution.getVariable("expression");
//        vars = new Map();
//        vars.put(camel);
//        vars.put(execution);
//        ONGL.execute(expression, vars);
//        execution.get

        try (ProducerTemplate tpl = camelCtx.createProducerTemplate()) {
            tpl.sendBodyAndProperty((String) execution.getVariable("endpoint"),
                    execution.getVariable("content"),
                    CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID, execution.getProcessInstanceId());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
