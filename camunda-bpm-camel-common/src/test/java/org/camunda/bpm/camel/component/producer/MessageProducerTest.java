package org.camunda.bpm.camel.component.producer;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;

import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Producer;
import org.assertj.core.api.Assertions;
import org.camunda.bpm.camel.BaseCamelTest;
import org.camunda.bpm.camel.component.CamundaBpmConstants;
import org.camunda.bpm.camel.component.CamundaBpmEndpoint;
import org.camunda.bpm.engine.runtime.Execution;
import org.camunda.bpm.engine.runtime.ExecutionQuery;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.runtime.ProcessInstanceQuery;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class MessageProducerTest extends BaseCamelTest {

  @Test
  void getSignalProcessProducerFromUri() throws Exception {
    CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
        CamundaBpmConstants.camundaBpmUri(
            "message?" + CamundaBpmConstants.ACTIVITY_ID_PARAMETER + "=" + "anActivityId"));
    Producer producer = endpoint.createProducer();
    Assertions.assertThat(producer).isInstanceOf(MessageProducer.class);
  }

  @Test
  void messageIsDeliveredCalled() throws Exception {
    ProcessInstance processInstance = mock(ProcessInstance.class);
    Mockito.when(processInstance.getProcessInstanceId()).thenReturn("theProcessInstanceId");
    Mockito.when(processInstance.getProcessDefinitionId()).thenReturn("theProcessDefinitionId");
    Mockito
        .when(runtimeService.startProcessInstanceByKey(
            eq("aProcessDefinitionKey"),
            anyMap()))
        .thenReturn(processInstance);

    CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
        CamundaBpmConstants.camundaBpmUri(
            "message?" + CamundaBpmConstants.ACTIVITY_ID_PARAMETER + "=" + "anActivityId"));
    Producer producer = endpoint.createProducer();
    Assertions.assertThat(producer).isInstanceOf(MessageProducer.class);
  }

  @Test
  void signalCalled() throws Exception {
    Exchange exchange = mock(Exchange.class);
    Message message = mock(Message.class);
    ExecutionQuery query = mock(ExecutionQuery.class);
    Execution execution = mock(Execution.class);

    Mockito.when(exchange.getIn()).thenReturn(message);
    Mockito.when(
        exchange.getProperty(CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID, String.class))
        .thenReturn("theProcessInstanceId");
    Mockito.when(runtimeService.createExecutionQuery()).thenReturn(query);
    Mockito.when(query.processInstanceId(anyString())).thenReturn(query);
    Mockito.when(query.activityId(anyString())).thenReturn(query);
    Mockito.when(query.singleResult()).thenReturn(execution);
    Mockito.when(execution.getId()).thenReturn("1234");

    CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
        CamundaBpmConstants.camundaBpmUri(
            "message?" + CamundaBpmConstants.ACTIVITY_ID_PARAMETER + "=" + "anActivityId"));
    Producer producer = endpoint.createProducer();

    producer.process(exchange);

    Mockito.verify(runtimeService).signal(anyString(), anyMap());
  }

  @Test
  void signalTransformBusinessKey() throws Exception {
    Exchange exchange = mock(Exchange.class);
    Message message = mock(Message.class);
    ExecutionQuery query = mock(ExecutionQuery.class);
    Execution execution = mock(Execution.class);
    ProcessInstanceQuery piQuery = mock(ProcessInstanceQuery.class);
    ProcessInstance processInstance = mock(ProcessInstance.class);

    Mockito.when(exchange.getIn()).thenReturn(message);
    Mockito
        .when(exchange.getProperty(CamundaBpmConstants.EXCHANGE_HEADER_BUSINESS_KEY, String.class))
        .thenReturn("theBusinessKey");

    Mockito.when(runtimeService.createProcessInstanceQuery()).thenReturn(piQuery);
    Mockito.when(runtimeService.createExecutionQuery()).thenReturn(query);
    Mockito.when(piQuery.processInstanceBusinessKey(anyString())).thenReturn(piQuery);
    Mockito.when(piQuery.singleResult()).thenReturn(processInstance);
    Mockito.when(processInstance.getId()).thenReturn("theProcessInstanceId");

    Mockito.when(query.processInstanceId(anyString())).thenReturn(query);
    Mockito.when(query.activityId(anyString())).thenReturn(query);
    Mockito.when(query.singleResult()).thenReturn(execution);

    CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
        CamundaBpmConstants.camundaBpmUri(
            "message?" + CamundaBpmConstants.ACTIVITY_ID_PARAMETER + "=" + "anActivityId"));
    Producer producer = endpoint.createProducer();

    producer.process(exchange);

    Mockito.verify(piQuery).processInstanceBusinessKey("theBusinessKey");
    Mockito.verify(query).processInstanceId("theProcessInstanceId");
  }

  @Test
  void messageProcessInstanceId() throws Exception {
    Exchange exchange = mock(Exchange.class);
    Message message = mock(Message.class);
    ExecutionQuery query = mock(ExecutionQuery.class);
    Execution execution = mock(Execution.class);

    Mockito.when(exchange.getIn()).thenReturn(message);
    Mockito
        .when(exchange.getProperty(
            CamundaBpmConstants.EXCHANGE_HEADER_PROCESS_INSTANCE_ID,
            String.class))
        .thenReturn("theProcessInstanceId");
    Mockito.when(runtimeService.createExecutionQuery()).thenReturn(query);
    Mockito.when(query.processInstanceId(anyString())).thenReturn(query);
    Mockito.when(query.messageEventSubscriptionName(anyString())).thenReturn(query);
    Mockito.when(query.singleResult()).thenReturn(execution);
    Mockito.when(execution.getId()).thenReturn("theExecutionId");

    CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
        CamundaBpmConstants.camundaBpmUri(
            "message?" + CamundaBpmConstants.MESSAGE_NAME_PARAMETER + "=" + "aMessageName"));
    Producer producer = endpoint.createProducer();

    producer.process(exchange);

    Mockito.verify(query).processInstanceId("theProcessInstanceId");
    Mockito.verify(query).messageEventSubscriptionName("aMessageName");

    Mockito.verify(runtimeService)
        .messageEventReceived(eq("aMessageName"), eq("theExecutionId"), anyMap());
  }

  @Test
  void messageBusinessKey() throws Exception {
    Exchange exchange = mock(Exchange.class);
    Message message = mock(Message.class);

    Mockito.when(exchange.getIn()).thenReturn(message);
    Mockito.when(exchange.getProperty(CamundaBpmConstants.EXCHANGE_HEADER_BUSINESS_KEY, String.class))
        .thenReturn("theBusinessKey");

    CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
        CamundaBpmConstants.camundaBpmUri(
            "message?" + CamundaBpmConstants.MESSAGE_NAME_PARAMETER + "=" + "aMessageName"));
    Producer producer = endpoint.createProducer();

    producer.process(exchange);

    @SuppressWarnings("rawtypes,unchecked")
    Class<Map<String, Object>> mapClass = (Class<Map<String, Object>>) (Class) Map.class;
    ArgumentCaptor<Map<String, Object>> correlationCaptor = ArgumentCaptor.forClass(mapClass);

    Mockito.verify(runtimeService).correlateMessage(eq("aMessageName"),
        eq("theBusinessKey"),
        correlationCaptor.capture(),
        anyMap());

    Assertions.assertThat(correlationCaptor.getValue()).isEmpty();
  }

  @Test
  void messageBusinessKeyCorrelationKey() throws Exception {
    Exchange exchange = mock(Exchange.class);
    Message message = mock(Message.class);

    final String BODY = "body";
    Mockito.when(message.getBody()).thenReturn(BODY);
    Mockito.when(exchange.getIn()).thenReturn(message);
    Mockito.when(exchange.getProperty(CamundaBpmConstants.EXCHANGE_HEADER_BUSINESS_KEY, String.class))
        .thenReturn("theBusinessKey");
    Mockito.when(exchange.getProperty(CamundaBpmConstants.EXCHANGE_HEADER_CORRELATION_KEY, String.class))
        .thenReturn("theCorrelationKey");

    CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
        CamundaBpmConstants.camundaBpmUri(
            "message?" + CamundaBpmConstants.MESSAGE_NAME_PARAMETER + "=aMessageName"
                + "&" + CamundaBpmConstants.CORRELATION_KEY_NAME_PARAMETER + "=aCorrelationKeyName"
                + "&" + CamundaBpmConstants.COPY_MESSAGE_BODY_AS_PROCESS_VARIABLE_PARAMETER + "=test"));

    Producer producer = endpoint.createProducer();

    producer.process(exchange);

    @SuppressWarnings({"rawtypes", "unchecked"})
    Class<Map<String, Object>> mapClass = (Class<Map<String, Object>>) (Class) Map.class;
    ArgumentCaptor<Map<String, Object>> correlationCaptor = ArgumentCaptor.forClass(mapClass);
    ArgumentCaptor<Map<String, Object>> variablesCaptor = ArgumentCaptor.forClass(mapClass);

    Mockito.verify(runtimeService).correlateMessage(eq("aMessageName"),
        eq("theBusinessKey"),
        correlationCaptor.capture(),
        variablesCaptor.capture());

    Assertions.assertThat(correlationCaptor.getValue()).hasSize(1);
    Assertions.assertThat(correlationCaptor.getValue())
        .containsEntry("aCorrelationKeyName", "theCorrelationKey");
    Assertions.assertThat(variablesCaptor.getValue()).hasSize(1);
    Assertions.assertThat(variablesCaptor.getValue()).containsEntry("test", BODY);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  void messageBusinessKeyCorrelationKeyType() throws Exception {
    Exchange exchange = mock(Exchange.class);
    Message message = mock(Message.class);

    Mockito.when(exchange.getIn()).thenReturn(message);
    Mockito
        .when(exchange.getProperty(
            CamundaBpmConstants.EXCHANGE_HEADER_BUSINESS_KEY,
            String.class))
        .thenReturn("theBusinessKey");

    Mockito
        .when(exchange.getProperty(
            CamundaBpmConstants.EXCHANGE_HEADER_CORRELATION_KEY,
            Integer.class))
        .thenReturn(15);

    Mockito
        .when(exchange.getProperty(
            CamundaBpmConstants.EXCHANGE_HEADER_CORRELATION_KEY_TYPE,
            String.class))
        .thenReturn("java.lang.Integer");

    CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
        CamundaBpmConstants.camundaBpmUri(
            "message?" + CamundaBpmConstants.MESSAGE_NAME_PARAMETER + "=aMessageName" + "&"
                + CamundaBpmConstants.CORRELATION_KEY_NAME_PARAMETER + "=aCorrelationKeyName" + "&"
                + CamundaBpmConstants.EXCHANGE_HEADER_CORRELATION_KEY_TYPE + "=java.lang.Integer"));

    Producer producer = endpoint.createProducer();

    producer.process(exchange);

    @SuppressWarnings({"rawtypes", "unchecked"})
    Class<Map<String, Object>> mapClass = (Class<Map<String, Object>>) (Class) Map.class;
    ArgumentCaptor<Map<String, Object>> correlationCaptor = ArgumentCaptor.forClass(mapClass);

    Mockito.verify(runtimeService).correlateMessage(eq("aMessageName"),
        eq("theBusinessKey"),
        correlationCaptor.capture(),
        anyMap());

    Assertions.assertThat(correlationCaptor.getValue()).hasSize(1);
    Assertions.assertThat(correlationCaptor.getValue()).containsEntry("aCorrelationKeyName", 15);

  }

  @Test
  void messageNoKey() throws Exception {
    Exchange exchange = mock(Exchange.class);
    Message message = mock(Message.class);

    Mockito.when(exchange.getIn()).thenReturn(message);

    CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
        CamundaBpmConstants.camundaBpmUri(
            "message?" + CamundaBpmConstants.MESSAGE_NAME_PARAMETER + "=" + "aMessageName"));
    Producer producer = endpoint.createProducer();

    producer.process(exchange);

    @SuppressWarnings({"rawtypes", "unchecked"})
    Class<Map<String, Object>> mapClass = (Class<Map<String, Object>>) (Class) Map.class;
    ArgumentCaptor<Map<String, Object>> correlationCaptor = ArgumentCaptor.forClass(mapClass);
    Mockito.verify(runtimeService).correlateMessage(eq("aMessageName"),
        correlationCaptor.capture(), anyMap());

    Assertions.assertThat(correlationCaptor.getValue()).isEmpty();
  }

  @Test
  void messageCorrelationKey() throws Exception {
    Exchange exchange = mock(Exchange.class);
    Message message = mock(Message.class);

    Mockito.when(exchange.getIn()).thenReturn(message);
    Mockito
        .when(exchange.getProperty(
            CamundaBpmConstants.EXCHANGE_HEADER_CORRELATION_KEY,
            String.class))
        .thenReturn("theCorrelationKey");

    CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
        CamundaBpmConstants.camundaBpmUri(
            "message?" + CamundaBpmConstants.MESSAGE_NAME_PARAMETER + "=aMessageName" + "&"
            + CamundaBpmConstants.CORRELATION_KEY_NAME_PARAMETER + "=aCorrelationKeyName"));
    Producer producer = endpoint.createProducer();

    producer.process(exchange);

    @SuppressWarnings("rawtypes,unchecked")
    Class<Map<String, Object>> mapClass = (Class<Map<String, Object>>) (Class) Map.class;
    ArgumentCaptor<Map<String, Object>> correlationCaptor = ArgumentCaptor.forClass(mapClass);
    Mockito.verify(runtimeService)
        .correlateMessage(
            eq("aMessageName"),
            correlationCaptor.capture(),
            anyMap());

    Assertions.assertThat(correlationCaptor.getValue()).hasSize(1);
    Assertions.assertThat(correlationCaptor.getValue())
        .containsEntry("aCorrelationKeyName", "theCorrelationKey");
  }

  @Test
  void shouldFailWithoutMessageActivityId() {
    CamundaBpmEndpoint endpoint = (CamundaBpmEndpoint) camelContext.getEndpoint(
        CamundaBpmConstants.camundaBpmUri("message"));
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class,
        endpoint::createProducer);
  }
}
