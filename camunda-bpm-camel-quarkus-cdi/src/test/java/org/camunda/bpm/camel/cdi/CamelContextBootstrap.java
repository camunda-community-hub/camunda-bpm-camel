package org.camunda.bpm.camel.cdi;

import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.camunda.bpm.camel.component.CamundaBpmComponent;
import org.camunda.bpm.engine.ProcessEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Camel context for CDI tests.
 * 
 * Since the @Startup annotation is *lazily* initalized we do not have a
 * guarantedd that the Camel context (components, ...) will be ready when
 * needed. This is why we use the ArquillianTestsProcessApplication.class to
 * make sure that the CamelContextBootstrap is *really* initialized at
 * application start.
 * 
 * Follow this link for background:
 * http://rmannibucau.wordpress.com/2012/12/11/ensure-some-applicationscoped-
 * beans-are-eagerly-initialized-with-javaee/
 */
@Singleton
@Startup
public class CamelContextBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(CamelContextBootstrap.class);

    @Inject
    CamelContext camelCtx;

    @Inject
    ProcessEngine processEngine;

    @PostConstruct
    public void init() throws Exception {
        LOG.info(">>");
        LOG.info(">> Starting Apache Camel's context");
        LOG.info(">>");

        LOG.info(">>");
        LOG.info(">> Registering camunda BPM component in Camel context");
        LOG.info(">>");
        CamundaBpmComponent component = new CamundaBpmComponent(processEngine);
        component.setCamelContext(camelCtx);
        camelCtx.addComponent("camunda-bpm", component);
    }

    public void addRoute(RouteBuilder route) throws Exception {
        LOG.info(">>");
        LOG.info(">> Registering Camel route");
        LOG.info(">>");
        camelCtx.addRoutes(route);
    }

    public void start() throws Exception {
        camelCtx.start();
        LOG.info(">>");
        LOG.info(">> Camel context started");
        LOG.info(">>");
    }

    @PreDestroy
    public void stop() throws Exception {
        camelCtx.stop();
        LOG.info(">> Camel context stopped");
    }

    public CamelContext getCamelContext() {
        return this.camelCtx;
    }

}
