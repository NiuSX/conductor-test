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
package com.netflix.conductor.core.execution.tasks;

import java.util.HashMap;
import java.util.Map;

import javax.validation.Validator;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.core.exception.TransientException;
import com.netflix.conductor.core.execution.StartWorkflowInput;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.operation.StartWorkflowOperation;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import com.fasterxml.jackson.databind.ObjectMapper;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_START_WORKFLOW;
import static com.netflix.conductor.model.TaskModel.Status.COMPLETED;
import static com.netflix.conductor.model.TaskModel.Status.FAILED;

/**
 * START_WORKFLOW 系统任务：用于在某个工作流内部"启动另一个（子）工作流"。
 * <p>
 * 与 SUB_WORKFLOW 任务不同：
 * <ul>
 *   <li>SUB_WORKFLOW：父工作流会等待子工作流完成，并把子工作流结果同步回父任务；</li>
 *   <li>START_WORKFLOW：只是"触发启动"另一个工作流，拿到其 workflowId 后本任务即 COMPLETED，
 *       不等待被启动的工作流结束（fire-and-forget）。</li>
 * </ul>
 * 它是一个异步系统任务（isAsync() == true）。
 */
@Component(TASK_TYPE_START_WORKFLOW) // 以任务类型名注册为 Spring Bean，供 SystemTaskRegistry 识别
public class StartWorkflow extends WorkflowSystemTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(StartWorkflow.class);

    /** 输出字段名：被启动工作流的 id */
    private static final String WORKFLOW_ID = "workflowId";
    /** 输入参数名：承载 StartWorkflowRequest 的键 */
    private static final String START_WORKFLOW_PARAMETER = "startWorkflow";

    private final ObjectMapper objectMapper;
    private final Validator validator;
    private final StartWorkflowOperation startWorkflowOperation;

    /** 构造器：注入 JSON 转换、参数校验、以及真正的启动操作组件 */
    public StartWorkflow(
            ObjectMapper objectMapper,
            Validator validator,
            StartWorkflowOperation startWorkflowOperation) {
        super(TASK_TYPE_START_WORKFLOW);
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.startWorkflowOperation = startWorkflowOperation;
    }

    /**
     * 系统任务的 start 回调：解析请求 → 启动目标工作流 → 设置任务结果状态。
     * <p>
     * 结果处理：
     * <ul>
     *   <li>成功：把新工作流 id 写入输出，任务置 COMPLETED；</li>
     *   <li>TransientException（可重试的后端瞬时错误）：不改状态，留待后续重试；</li>
     *   <li>其他异常：任务置 FAILED 并记录原因。</li>
     * </ul>
     */
    @Override
    public void start(
            WorkflowModel workflow, TaskModel taskModel, WorkflowExecutor workflowExecutor) {
        // 解析并校验输入中的 StartWorkflowRequest；解析失败时内部已把任务置为 FAILED
        StartWorkflowRequest request = getRequest(taskModel);
        if (request == null) {
            return; // 解析/校验失败，任务状态已在 getRequest 中设置，直接返回
        }

        // 若请求未指定 taskToDomain，则继承当前（父）工作流的 taskToDomain 配置
        if (request.getTaskToDomain() == null || request.getTaskToDomain().isEmpty()) {
            Map<String, String> workflowTaskToDomainMap = workflow.getTaskToDomain();
            if (workflowTaskToDomainMap != null) {
                request.setTaskToDomain(new HashMap<>(workflowTaskToDomainMap));
            }
        }

        // 若请求未设置 correlationId，则沿用当前（父）工作流的 correlationId，保持链路关联
        request.setCorrelationId(
                StringUtils.defaultIfBlank(
                        request.getCorrelationId(), workflow.getCorrelationId()));

        try {
            // 真正启动目标工作流，返回其 workflowId
            String workflowId = startWorkflow(request, workflow.getWorkflowId());
            // 把新工作流 id 写入任务输出，标记任务完成
            taskModel.addOutput(WORKFLOW_ID, workflowId);
            taskModel.setStatus(COMPLETED);
        } catch (TransientException te) {
            // 瞬时后端错误：不改变任务状态（既非完成也非失败），等待后续重试
            LOGGER.info(
                    "A transient backend error happened when task {} in {} tried to start workflow {}.",
                    taskModel.getTaskId(),
                    workflow.toShortString(),
                    request.getName());
        } catch (Exception ae) {
            // 其他异常：视为不可恢复失败，记录失败原因
            taskModel.setStatus(FAILED);
            taskModel.setReasonForIncompletion(ae.getMessage());
            LOGGER.error(
                    "Error starting workflow: {} from workflow: {}",
                    request.getName(),
                    workflow.toShortString(),
                    ae);
        }
    }

    /**
     * 从任务输入数据中解析出 StartWorkflowRequest 并做校验。
     * 解析或校验失败时，会把任务置为 FAILED 并写入失败原因，然后返回 null。
     *
     * @param taskModel 当前 START_WORKFLOW 任务
     * @return 解析成功的请求；失败时返回 null
     */
    private StartWorkflowRequest getRequest(TaskModel taskModel) {
        Map<String, Object> taskInput = taskModel.getInputData();

        StartWorkflowRequest startWorkflowRequest = null;

        // 输入中必须包含 'startWorkflow' 参数
        if (taskInput.get(START_WORKFLOW_PARAMETER) == null) {
            taskModel.setStatus(FAILED);
            taskModel.setReasonForIncompletion(
                    "Missing '" + START_WORKFLOW_PARAMETER + "' in input data.");
        } else {
            try {
                // 把输入中的 Map 结构转换成 StartWorkflowRequest 对象
                startWorkflowRequest =
                        objectMapper.convertValue(
                                taskInput.get(START_WORKFLOW_PARAMETER),
                                StartWorkflowRequest.class);

                // 对请求做 Bean Validation 校验（如 name 非空等约束）
                var violations = validator.validate(startWorkflowRequest);
                if (!violations.isEmpty()) {
                    // 汇总所有校验错误到失败原因中
                    StringBuilder reasonForIncompletion =
                            new StringBuilder(START_WORKFLOW_PARAMETER)
                                    .append(" validation failed. ");
                    for (var violation : violations) {
                        reasonForIncompletion
                                .append("'")
                                .append(violation.getPropertyPath().toString())
                                .append("' -> ")
                                .append(violation.getMessage())
                                .append(". ");
                    }
                    taskModel.setStatus(FAILED);
                    taskModel.setReasonForIncompletion(reasonForIncompletion.toString());
                    startWorkflowRequest = null;
                }
            } catch (IllegalArgumentException e) {
                // 转换失败（输入结构无法映射为 StartWorkflowRequest）
                LOGGER.error("Error reading StartWorkflowRequest for {}", taskModel, e);
                taskModel.setStatus(FAILED);
                taskModel.setReasonForIncompletion(
                        "Error reading StartWorkflowRequest. " + e.getMessage());
            }
        }

        return startWorkflowRequest;
    }

    /**
     * 调用 StartWorkflowOperation 真正启动工作流。
     *
     * @param request    启动请求
     * @param workflowId 触发本次启动的当前工作流 id（作为 triggeringWorkflowId 记录来源）
     * @return 新启动工作流的 id
     */
    private String startWorkflow(StartWorkflowRequest request, String workflowId) {
        StartWorkflowInput input = new StartWorkflowInput(request);
        // 记录触发方工作流 id，便于追溯工作流之间的触发关系
        input.setTriggeringWorkflowId(workflowId);
        return startWorkflowOperation.execute(input);
    }

    /** 标记为异步系统任务：由 scheduleTask 入队后异步执行，而非在 decide 中同步执行 */
    @Override
    public boolean isAsync() {
        return true;
    }
}