package org.camunda.bpm.camel.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.ProducerTemplate;
import org.assertj.core.api.Assertions;
import org.camunda.bpm.camel.BaseCamelTest;
import org.camunda.bpm.camel.component.CamundaBpmConstants;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.impl.context.BpmnExecutionContext;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;


class CamelServiceTest extends BaseCamelTest {

  protected CamelServiceCommonImpl service;
  protected ProducerTemplate producerTemplate;
  protected ExecutionEntity execution;

  private MockedStatic<Context> contextMock;

  @BeforeEach
  public void setupService() {
    service = new CamelServiceCommonImpl() {
      @Override
      public void setProcessEngine(ProcessEngine processEngine) {
        this.processEngine = processEngine;
      }

      @Override
      public void setCamelContext(CamelContext camelContext) {
        this.camelContext = camelContext;
      }
    };
    service.setProcessEngine(processEngine);
    CamelContext camelContext = Mockito.mock(CamelContext.class);
    service.setCamelContext(camelContext);

    producerTemplate = Mockito.mock(ProducerTemplate.class);
    Mockito.when(camelContext.createProducerTemplate()).thenReturn(producerTemplate);

    BpmnExecutionContext executionContext = Mockito.mock(BpmnExecutionContext.class);
    execution = Mockito.mock(ExecutionEntity.class);

    Mockito.when(executionContext.getExecution()).thenReturn(execution);
    Mockito.when(execution.getProcessInstanceId()).thenReturn("theProcessInstanceId");
    Mockito.when(execution.getBusinessKey()).thenReturn("theBusinessKey");
    Mockito.when(execution.getVariable(anyString())).thenReturn("theVariable");

    contextMock = Mockito.mockStatic(Context.class);
    contextMock
        .when(Context::getBpmnExecutionContext)
        .thenReturn(executionContext);
  }

  @AfterEach
  void teardownService() {
    contextMock.close();
  }

  @Test
  void testSendToEndpoint() throws Exception {
    Exchange send = Mockito.mock(Exchange.class);
    Message message = Mockito.mock(Message.class);
    Mockito.when(send.getIn()).thenReturn(message);
    Mockito.when(producerTemplate.send(anyString(), any(Exchange.class)))
        .thenReturn(send);

    ArgumentCaptor<Exchange> exchangeCaptor = ArgumentCaptor
        .forClass(Exchange.class);

    service.sendTo("what/ever");

    Mockito.verify(producerTemplate).send(anyString(), exchangeCaptor.capture());
    Mockito.verify(execution).getVariableNames();

    Assertions
        .assertThat(exchangeCaptor
            .getValue()
            .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_BUSINESS_KEY))
        .isEqualTo("theBusinessKey");
    Assertions
        .assertThat(exchangeCaptor
            .getValue()
            .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_CORRELATION_KEY))
        .isNull();
    Assertions
        .assertThat(exchangeCaptor
            .getValue()
            .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID))
        .isEqualTo("theProcessInstanceId");
  }

  @Test
  void testSendToEndpointWithNoVariables() throws Exception {
    Exchange send = Mockito.mock(Exchange.class);
    Message message = Mockito.mock(Message.class);
    Mockito.when(send.getIn()).thenReturn(message);
    Mockito.when(producerTemplate.send(anyString(), any(Exchange.class))).thenReturn(send);

    ArgumentCaptor<Exchange> exchangeCaptor = ArgumentCaptor.forClass(Exchange.class);

    service.sendTo("what/ever", "");

    Mockito.verify(producerTemplate).send(anyString(), exchangeCaptor.capture());
    Mockito.verify(execution, never()).getVariableNames();
  }

  @Test
  void testSendToEndpointWithOneVariable() throws Exception {
    Exchange send = Mockito.mock(Exchange.class);
    Message message = Mockito.mock(Message.class);
    Mockito.when(send.getIn()).thenReturn(message);
    Mockito.when(producerTemplate.send(anyString(), any(Exchange.class))).thenReturn(send);

    ArgumentCaptor<Exchange> exchangeCaptor = ArgumentCaptor.forClass(Exchange.class);

    service.sendTo("what/ever", "varName");

    Mockito.verify(producerTemplate).send(anyString(), exchangeCaptor.capture());
    Mockito.verify(execution, never()).getVariableNames();
    Mockito.verify(execution).getVariable("varName");
  }

  @Test
  void testSendToEndpointWithAlleVariables() throws Exception {
    Exchange send = Mockito.mock(Exchange.class);
    Message message = Mockito.mock(Message.class);
    Mockito.when(send.getIn()).thenReturn(message);
    Mockito.when(producerTemplate.send(anyString(), any(Exchange.class))).thenReturn(send);

    ArgumentCaptor<Exchange> exchangeCaptor = ArgumentCaptor.forClass(Exchange.class);

    service.sendTo("what/ever", null);

    Mockito.verify(producerTemplate).send(anyString(), exchangeCaptor.capture());
    Mockito.verify(execution).getVariableNames();
  }

  @Test
  void testSendToEndpointWithCorrelation() throws Exception {
    Exchange send = Mockito.mock(Exchange.class);
    Message message = Mockito.mock(Message.class);
    Mockito.when(send.getIn()).thenReturn(message);
    Mockito.when(producerTemplate.send(anyString(), any(Exchange.class))).thenReturn(send);

    ArgumentCaptor<Exchange> exchangeCaptor = ArgumentCaptor.forClass(Exchange.class);

    service.sendTo("what/ever", null, "theCorrelationKey");

    Mockito.verify(producerTemplate).send(anyString(), exchangeCaptor.capture());

    Assertions
        .assertThat(exchangeCaptor
            .getValue()
            .getProperty(CamundaBpmConstants.EXCHANGE_HEADER_CORRELATION_KEY))
        .isEqualTo("theCorrelationKey");
  }
}