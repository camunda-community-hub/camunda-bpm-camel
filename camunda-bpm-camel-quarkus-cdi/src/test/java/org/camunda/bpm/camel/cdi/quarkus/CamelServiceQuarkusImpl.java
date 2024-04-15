package org.camunda.bpm.camel.cdi.quarkus;

import org.camunda.bpm.camel.cdi.CamelServiceImpl;
import jakarta.enterprise.context.Dependent;

/**
 * Quarkus needs a @Dependent or @ApplicationScoped annotation to instantiate a class.
 * The annotation @Named from CamelServiceImpl is not sufficient.
 */
@Dependent
public class CamelServiceQuarkusImpl extends CamelServiceImpl {
}
