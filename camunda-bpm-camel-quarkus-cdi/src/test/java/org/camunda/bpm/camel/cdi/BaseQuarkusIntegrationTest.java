package org.camunda.bpm.camel.cdi;

import org.camunda.bpm.camel.cdi.quarkus.LogServiceCdiImpl;
import org.camunda.bpm.camel.common.CamelService;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.TaskService;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;

public class BaseQuarkusIntegrationTest {

    @Inject
    RepositoryService repositoryService;

    @Inject
    RuntimeService runtimeService;

    @Inject
    TaskService taskService;

    @Inject
    HistoryService historyService;

    @Inject
    CamelContextBootstrap camelContextBootstrap;

    @Inject
    @Identifier("cdiLog")
    LogServiceCdiImpl log;

    @Inject
    CamelService camelService;

    protected void deployProcess(String processFilePath) {
        // Create a new deployment
        repositoryService.createDeployment()
                .addClasspathResource(processFilePath)// Filename of the process model
                .enableDuplicateFiltering(true)// No redeployment when process model remains unchanged
                .deploy();
    }

}
