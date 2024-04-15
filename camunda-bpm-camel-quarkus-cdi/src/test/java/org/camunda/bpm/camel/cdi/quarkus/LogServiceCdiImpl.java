package org.camunda.bpm.camel.cdi.quarkus;

import java.io.Serializable;
import org.camunda.bpm.camel.spring.util.LogServiceImpl;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Singleton;

@Singleton
@Identifier("cdiLog")
public class LogServiceCdiImpl extends LogServiceImpl implements Serializable {

}
