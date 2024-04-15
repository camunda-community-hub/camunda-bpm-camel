package org.camunda.bpm.camel.cdi;

import org.apache.camel.CamelContext;
import org.camunda.bpm.camel.common.CamelServiceCommonImpl;
import org.camunda.bpm.engine.ProcessEngine;

import jakarta.inject.Inject;
import jakarta.inject.Named;

@Named("camel")
public class CamelServiceImpl extends CamelServiceCommonImpl {

  @Inject
  public void setProcessEngine(ProcessEngine processEngine) {
    this.processEngine = processEngine;
  }

  @Inject
  public void setCamelContext(CamelContext camelContext) {
    this.camelContext = camelContext;
  }
}