package org.camunda.bpm.camel.component.producer;

import org.apache.camel.Exchange;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultExchange;
import org.assertj.core.api.Assertions;
import org.camunda.bpm.camel.BaseCamelTest;
import org.camunda.bpm.camel.component.CamundaBpmConstants;
import org.camunda.bpm.camel.component.CamundaBpmEndpoint;
import org.camunda.bpm.engine.runtime.ProcessInstanceWithVariables;
import org.camunda.bpm.engine.runtime.ProcessInstantiationBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;

class StartProcessProducerTest extends BaseCamelTest {

    @Test
    void getStartProcessProducerFromUri() throws Exception {
        CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
            CamundaBpmConstants.camundaBpmUri(
                "start?" + CamundaBpmConstants.PROCESS_DEFINITION_KEY_PARAMETER + "=aProcessDefinitionKey"));
        Producer producer = endpoint.createProducer();
        Assertions.assertThat(producer).isInstanceOf(StartProcessProducer.class);
    }

    @Test
    void createProcessInstanceByKeyShouldBeCalled() throws Exception {
        ProcessInstanceWithVariables processInstance =
            Mockito.mock(ProcessInstanceWithVariables.class);
        ProcessInstantiationBuilder processInstantiationBuilder =
            Mockito.mock(ProcessInstantiationBuilder.class);
        Mockito.when(processInstance.getProcessInstanceId()).thenReturn("theProcessInstanceId");
        Mockito.when(processInstance.getProcessDefinitionId())
            .thenReturn("theProcessDefinitionId");
        Mockito.when(runtimeService.createProcessInstanceByKey("aProcessDefinitionKey"))
            .thenReturn(processInstantiationBuilder);
        Mockito.when(processInstantiationBuilder.setVariables(anyMap()))
            .thenReturn(processInstantiationBuilder);
        Mockito.when(processInstantiationBuilder.executeWithVariablesInReturn())
            .thenReturn(processInstance);

        CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
            CamundaBpmConstants.camundaBpmUri(
                "start?" + CamundaBpmConstants.PROCESS_DEFINITION_KEY_PARAMETER + "=aProcessDefinitionKey"));
        StartProcessProducer producer = (StartProcessProducer) endpoint.createProducer();
        Exchange exchange = new DefaultExchange(camelContext);
        producer.process(exchange);

        Mockito.verify(runtimeService, times(1))
            .createProcessInstanceByKey("aProcessDefinitionKey");
        Assertions
            .assertThat(exchange.getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_DEFINITION_ID))
            .isEqualTo("theProcessDefinitionId");
        Assertions
            .assertThat(exchange.getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID))
            .isEqualTo("theProcessInstanceId");
    }

    @Test
    void createProcessInstanceByKeyWithBusinessKeyShouldBeCalled() throws Exception {
        ProcessInstanceWithVariables processInstance =
            Mockito.mock(ProcessInstanceWithVariables.class);
        ProcessInstantiationBuilder processInstantiationBuilder =
            Mockito.mock(ProcessInstantiationBuilder.class);
        Mockito.when(processInstance.getProcessInstanceId()).thenReturn("theProcessInstanceId");
        Mockito.when(processInstance.getProcessDefinitionId()).thenReturn("theProcessDefinitionId");
        Mockito.when(processInstance.getBusinessKey()).thenReturn("aBusinessKey");
        Mockito.when(runtimeService.createProcessInstanceByKey("aProcessDefinitionKey"))
                .thenReturn(processInstantiationBuilder);
        Mockito.when(processInstantiationBuilder.setVariables(anyMap()))
            .thenReturn(processInstantiationBuilder);
        Mockito.when(processInstantiationBuilder.businessKey(anyString()))
            .thenReturn(processInstantiationBuilder);
        Mockito.when(processInstantiationBuilder.executeWithVariablesInReturn())
            .thenReturn(processInstance);

        CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
            CamundaBpmConstants.camundaBpmUri(
                "start?" + CamundaBpmConstants.PROCESS_DEFINITION_KEY_PARAMETER + "=aProcessDefinitionKey"));
        StartProcessProducer producer = (StartProcessProducer) endpoint.createProducer();
        Exchange exchange = new DefaultExchange(camelContext);
        exchange.setProperty(CamundaBpmConstants.EXCHANGE_HEADER_BUSINESS_KEY, "aBusinessKey");
        producer.process(exchange);

        Mockito.verify(runtimeService, times(1))
            .createProcessInstanceByKey("aProcessDefinitionKey");
        Mockito.verify(processInstantiationBuilder, times(1))
            .businessKey("aBusinessKey");
        Assertions
            .assertThat(exchange.getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_DEFINITION_ID))
            .isEqualTo("theProcessDefinitionId");
        Assertions
            .assertThat(exchange.getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID))
            .isEqualTo("theProcessInstanceId");
        Assertions
            .assertThat(exchange.getProperty(CamundaBpmConstants.EXCHANGE_HEADER_BUSINESS_KEY))
            .isEqualTo("aBusinessKey");
    }
}
