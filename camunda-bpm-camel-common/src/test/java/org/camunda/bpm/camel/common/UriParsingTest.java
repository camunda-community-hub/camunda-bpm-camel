package org.camunda.bpm.camel.common;

import org.apache.camel.ResolveEndpointFailedException;
import org.camunda.bpm.camel.BaseCamelTest;
import org.camunda.bpm.camel.component.CamundaBpmConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Objects;

class UriParsingTest extends BaseCamelTest {
    @Test
    void testThatCamelContextIsInitialized() {
        camelContext.getComponent(CamundaBpmConstants.CAMUNDA_BPM_CAMEL_URI_SCHEME);
    }

    @Test
    void testGetCamundaEndpointWithUnknownUriExtension() {
        String uri = Objects.requireNonNull(CamundaBpmConstants.camundaBpmUri("what/ever"));
        Assertions.assertThrows(
            ResolveEndpointFailedException.class,
            () -> camelContext.getEndpoint(uri));
    }
}