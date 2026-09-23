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

            // 记录工作流启动成功的监控指标
            Monitors.recordWorkflowStartSuccess(
                    workflow.getWorkflowName(),
                    String.valueOf(workflow.getWorkflowVersion()),
                    workflow.getOwnerApp());

            // 返回本次启动创建的工作流实例 ID，供调用方后续使用（如查询状态、关联任务等）
            return workflowId;
        } catch (Exception e) {
            // 捕获启动过程中的任何异常，进行错误监控上报与资源清理

            // 记录工作流启动失败的监控指标
            // 参数说明：
            //   - workflowDefinition.getName()             : 工作流定义的名称（注意此处用的是定义对象而非实例对象）
            //   - WorkflowContext.get().getClientApp()     : 当前线程上下文中记录的客户端应用标识，便于定位是哪个调用方触发的失败
            Monitors.recordWorkflowStartError(
                    workflowDefinition.getName(), WorkflowContext.get().getClientApp());

            // 记录错误日志，包含工作流定义名称与完整异常堆栈，便于问题排查
            LOGGER.error("Unable to start workflow: {}", workflowDefinition.getName(), e);

            // 启动失败：尝试清理已创建的工作流（可能只创建了一半）
            // 说明：由于 createAndEvaluate 内部可能包含多步操作（如先写库再发事件），
            //      若中途抛出异常，可能已在存储层留下不完整的数据，因此需要尽力回滚清理。
            try {
                // 调用 DAO 门面删除该工作流记录
                // 第二个参数 false 通常表示"非强制删除"或"不做级联删除"等语义（具体取决于实现），
                // 即仅移除主记录，不递归清理关联的子任务/子流程，避免误删或性能开销。
                executionDAOFacade.removeWorkflow(workflowId, false);
            } catch (Exception rwe) {
                // 清理操作本身也可能失败（如数据库连接异常、记录已被其他线程删除等），
                // 此处单独捕获并记录日志，避免清理异常覆盖原始业务异常（e），
                // 保证最终抛出给上层的仍是导致启动失败的根本原因。
                LOGGER.error("Could not remove the workflowId: " + workflowId, rwe);
            }

            // 将原始异常 e 向上抛出，由上层调用者决定如何处理（如返回错误码、重试等）
            // 注意：这里抛出的是原始异常而非清理异常，确保调用方能看到真正的失败原因
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