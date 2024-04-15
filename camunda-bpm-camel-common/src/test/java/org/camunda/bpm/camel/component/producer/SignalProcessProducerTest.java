package org.camunda.bpm.camel.component.producer;

import org.apache.camel.Endpoint;
import org.apache.camel.Producer;
import org.assertj.core.api.Assertions;
import org.camunda.bpm.camel.BaseCamelTest;
import org.camunda.bpm.camel.component.CamundaBpmConstants;
import org.camunda.bpm.camel.component.CamundaBpmEndpoint;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;

class SignalProcessProducerTest extends BaseCamelTest {

    @Test
    void getSignalProcessProducerFromUri() throws Exception {
        CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
            CamundaBpmConstants.camundaBpmUri(
                "signal?" + CamundaBpmConstants.PROCESS_DEFINITION_KEY_PARAMETER + "=aProcessDefinitionKey"
                    + "&" + CamundaBpmConstants.ACTIVITY_ID_PARAMETER + "=anActivityId"));
        Producer producer = endpoint.createProducer();
        Assertions.assertThat(producer).isInstanceOf(MessageProducer.class);
    }

    @Test
    void noActivityIdParameterShouldThrowException() {
        Endpoint endpoint = camelContext.getEndpoint(
            CamundaBpmConstants.camundaBpmUri("signal?" + CamundaBpmConstants.PROCESS_DEFINITION_KEY_PARAMETER + "=aProcessDefinitionKey"));
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            endpoint::createProducer);
    }

    @Test
    void signalShouldBeCalled() throws Exception {
        ProcessInstance processInstance = Mockito.mock(ProcessInstance.class);
        Mockito.when(processInstance.getProcessInstanceId()).thenReturn("theProcessInstanceId");
        Mockito.when(processInstance.getProcessDefinitionId()).thenReturn("theProcessDefinitionId");
        Mockito
            .when(runtimeService.startProcessInstanceByKey(
                eq("aProcessDefinitionKey"),
                anyMap()))
            .thenReturn(processInstance);

        CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
            CamundaBpmConstants.camundaBpmUri(
                "signal?" + CamundaBpmConstants.PROCESS_DEFINITION_KEY_PARAMETER + "=aProcessDefinitionKey"
                    + "&" + CamundaBpmConstants.ACTIVITY_ID_PARAMETER + "=anActivityId"));
        Producer producer = endpoint.createProducer();
        Assertions.assertThat(producer).isInstanceOf(MessageProducer.class);
    }

    @Test
    void signalWithBusinessKeyShouldBeCalled() throws Exception {
        ProcessInstance processInstance = Mockito.mock(ProcessInstance.class);
        Mockito.when(processInstance.getProcessInstanceId()).thenReturn("theProcessInstanceId");
        Mockito.when(processInstance.getProcessDefinitionId())
            .thenReturn("theProcessDefinitionId");
        Mockito
            .when(runtimeService.startProcessInstanceByKey(
                eq("aProcessDefinitionKey"),
                anyMap()))
            .thenReturn(processInstance);

        CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
            CamundaBpmConstants.camundaBpmUri(
                "signal?" + CamundaBpmConstants.PROCESS_DEFINITION_KEY_PARAMETER + "=aProcessDefinitionKey"
                    + "&" + CamundaBpmConstants.ACTIVITY_ID_PARAMETER + "=anActivityId"));
        Producer producer = endpoint.createProducer();
        Assertions.assertThat(producer).isInstanceOf(MessageProducer.class);
    }

}
