/*
 * Copyright 2022 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.operation;

import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.WorkflowContext;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.event.WorkflowCreationEvent;
import com.netflix.conductor.core.event.WorkflowEvaluationEvent;
import com.netflix.conductor.core.exception.TransientException;
import com.netflix.conductor.core.execution.StartWorkflowInput;
import com.netflix.conductor.core.metadata.MetadataMapperService;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.WorkflowModel;
import com.netflix.conductor.service.ExecutionLockService;

/**
 * 启动工作流操作：把"启动工作流的请求"转化为一个持久化的工作流实例。
 *
 * 它是 WorkflowOperation 模式的一个实现，封装了启动工作流的完整流程：
 * 1. 解析工作流定义（从请求或 MetadataService）
 * 2. 填充任务定义
 * 3. 校验输入
 * 4. 生成 workflowId、构造 WorkflowModel
 * 5. 加锁、持久化、发布评估事件（触发首次 decide）
 *
 * 为什么单独抽成一个 Operation 类？
 * - 启动逻辑较复杂，抽出来让 WorkflowServiceImpl 保持薄封装
 * - 可以被多种入口复用：直接调用 execute()，或监听 WorkflowCreationEvent 事件
 */
@Component
public class StartWorkflowOperation implements WorkflowOperation<StartWorkflowInput, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StartWorkflowOperation.class);

    private final MetadataMapperService metadataMapperService;
    private final IDGenerator idGenerator;
    private final ParametersUtils parametersUtils;
    private final ExecutionDAOFacade executionDAOFacade;
    private final ExecutionLockService executionLockService;
    private final ApplicationEventPublisher eventPublisher;

    public StartWorkflowOperation(
            MetadataMapperService metadataMapperService,
            IDGenerator idGenerator,
            ParametersUtils parametersUtils,
            ExecutionDAOFacade executionDAOFacade,
            ExecutionLockService executionLockService,
            ApplicationEventPublisher eventPublisher) {
        this.metadataMapperService = metadataMapperService;
        this.idGenerator = idGenerator;
        this.parametersUtils = parametersUtils;
        this.executionDAOFacade = executionDAOFacade;
        this.executionLockService = executionLockService;
        this.eventPublisher = eventPublisher;
    }

    /**
     * 执行入口：WorkflowService 通过它启动工作流。
     *
     * @param input 启动工作流的输入
     * @return 工作流实例 id
     */
    @Override
    public String execute(StartWorkflowInput input) {
        return startWorkflow(input);
    }

    /**
     * 监听 WorkflowCreationEvent 事件，异步启动工作流。
     * 用于"失败工作流"等由事件触发的启动场景。
     */
    @EventListener(WorkflowCreationEvent.class)
    public void handleWorkflowCreationEvent(WorkflowCreationEvent workflowCreationEvent) {
        startWorkflow(workflowCreationEvent.getStartWorkflowInput());
    }

    /**
     * 启动工作流的核心实现。
     *
     * 流程：
     * 1. 解析定义（请求内联 or 从 MetadataService 查）
     * 2. 填充任务定义（TaskDef、子工作流版本）
     * 3. 校验输入
     * 4. 生成 workflowId、构造 WorkflowModel
     * 5. 加锁 → 持久化 → 发布评估事件
     *
     * @param input 启动输入
     * @return 工作流实例 id
     */
    private String startWorkflow(StartWorkflowInput input) {
        WorkflowDef workflowDefinition;

        // 1. 解析工作流定义
        if (input.getWorkflowDefinition() == null) {
            // 请求没带定义：从 MetadataService 查（按 name + version）
            workflowDefinition =
                    metadataMapperService.lookupForWorkflowDefinition(
                            input.getName(), input.getVersion());
        } else {
            // 请求内联了定义：直接用
            workflowDefinition = input.getWorkflowDefinition();
        }

        // 2. 填充任务定义（TaskDef、子工作流版本等）
        workflowDefinition = metadataMapperService.populateTaskDefinitions(workflowDefinition);

        // 3. 校验输入
        Map<String, Object> workflowInput = input.getWorkflowInput();
        String externalInputPayloadStoragePath = input.getExternalInputPayloadStoragePath();
        validateWorkflow(workflowDefinition, workflowInput, externalInputPayloadStoragePath);

        // 4. 生成 workflowId（若请求已指定则复用）
        String workflowId =
                Optional.ofNullable(input.getWorkflowId()).orElseGet(idGenerator::generate);

        // 5. 构造 WorkflowModel
        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowId(workflowId);
        workflow.setCorrelationId(input.getCorrelationId());
        workflow.setPriority(input.getPriority() == null ? 0 : input.getPriority());
        workflow.setWorkflowDefinition(workflowDefinition);
        workflow.setStatus(WorkflowModel.Status.RUNNING);
        workflow.setParentWorkflowId(input.getParentWorkflowId());
        workflow.setParentWorkflowTaskId(input.getParentWorkflowTaskId());
        // 记录发起方（从线程上下文获取）
        workflow.setOwnerApp(WorkflowContext.get().getClientApp());
        workflow.setCreateTime(System.currentTimeMillis());
        workflow.setUpdatedBy(null);
        workflow.setUpdatedTime(null);
        workflow.setEvent(input.getEvent());
        workflow.setTaskToDomain(input.getTaskToDomain());
        // 从定义里带出初始变量
        workflow.setVariables(workflowDefinition.getVariables());

        // 设置输入：内联输入则解析，否则记录外部存储路径
        if (workflowInput != null && !workflowInput.isEmpty()) {
            Map<String, Object> parsedInput =
                    parametersUtils.getWorkflowInput(workflowDefinition, workflowInput);
            workflow.setInput(parsedInput);
        } else {
            workflow.setExternalInputPayloadStoragePath(externalInputPayloadStoragePath);
        }

        try {
            // 6. 加锁持久化 + 发布评估事件
            createAndEvaluate(workflow);
            Monitors.recordWorkflowStartSuccess(
                    workflow.getWorkflowName(),
                    String.valueOf(workflow.getWorkflowVersion()),
                    workflow.getOwnerApp());
            return workflowId;
        } catch (Exception e) {
            Monitors.recordWorkflowStartError(
                    workflowDefinition.getName(), WorkflowContext.get().getClientApp());
            LOGGER.error("Unable to start workflow: {}", workflowDefinition.getName(), e);

            // 启动失败：尝试清理已创建的工作流（可能只创建了一半）
            try {
                executionDAOFacade.removeWorkflow(workflowId, false);
            } catch (Exception rwe) {
                LOGGER.error("Could not remove the workflowId: " + workflowId, rwe);
            }
            throw e;
        }
    }

    /*
     * 获取并持有锁，直到工作流创建动作完成（主存储和二级存储都写完）。
     * 这是为了确保"工作流创建"动作先于对该工作流的任何其他动作。
     */
    private void createAndEvaluate(WorkflowModel workflow) {
        if (!executionLockService.acquireLock(workflow.getWorkflowId())) {
            throw new TransientException("Error acquiring lock when creating workflow: {}");
        }
        try {
            // 持久化工作流到数据库
            executionDAOFacade.createWorkflow(workflow);
            LOGGER.debug(
                    "A new instance of workflow: {} created with id: {}",
                    workflow.getWorkflowName(),
                    workflow.getWorkflowId());
            // 填充工作流和任务的 payload 数据（如从外部存储加载）
            executionDAOFacade.populateWorkflowAndTaskPayloadData(workflow);
            // 发布评估事件，触发首次 decide（异步推进工作流）
            eventPublisher.publishEvent(new WorkflowEvaluationEvent(workflow));
        } finally {
            executionLockService.releaseLock(workflow.getWorkflowId());
        }
    }

    /**
     * 启动工作流的校验。
     *
     * @throws IllegalArgumentException 校验失败
     */
    private void validateWorkflow(
            WorkflowDef workflowDef,
            Map<String, Object> workflowInput,
            String externalStoragePath) {
        // 输入不能为 null：要么有内联输入，要么有外部存储路径
        if (workflowInput == null && StringUtils.isBlank(externalStoragePath)) {
            LOGGER.error("The input for the workflow '{}' cannot be NULL", workflowDef.getName());
            Monitors.recordWorkflowStartError(
                    workflowDef.getName(), WorkflowContext.get().getClientApp());

            throw new IllegalArgumentException("NULL input passed when starting workflow");
        }
    }
}