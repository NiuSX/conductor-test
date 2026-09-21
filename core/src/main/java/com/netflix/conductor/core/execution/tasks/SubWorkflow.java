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

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.exception.NonTransientException;
import com.netflix.conductor.core.exception.TransientException;
import com.netflix.conductor.core.execution.StartWorkflowInput;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.operation.StartWorkflowOperation;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import com.fasterxml.jackson.databind.ObjectMapper;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_SUB_WORKFLOW;

/**
 * SUB_WORKFLOW 系统任务：把一个子工作流作为父工作流中的一个任务来执行。
 *
 * 它是异步系统任务（isAsync=true，isAsyncComplete=true），运行机制：
 * 1. start() 时启动子工作流，记录 subWorkflowId，任务状态置为 IN_PROGRESS
 * 2. 子工作流完成后，由子工作流的 completeWorkflow 逻辑反向更新该任务状态，
 *    从而避免父工作流周期性轮询子工作流状态
 * 3. cancel() 时终止子工作流
 */
@Component(TASK_TYPE_SUB_WORKFLOW)
public class SubWorkflow extends WorkflowSystemTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubWorkflow.class);
    /** 输出字段名：子工作流 id（向后兼容） */
    private static final String SUB_WORKFLOW_ID = "subWorkflowId";

    private final ObjectMapper objectMapper;
    private final StartWorkflowOperation startWorkflowOperation;

    public SubWorkflow(ObjectMapper objectMapper, StartWorkflowOperation startWorkflowOperation) {
        super(TASK_TYPE_SUB_WORKFLOW);
        this.objectMapper = objectMapper;
        this.startWorkflowOperation = startWorkflowOperation;
    }

    /**
     * 启动子工作流任务：
     * - 从任务输入中读取子工作流名称、版本、定义（可选）、taskToDomain、输入参数
     * - 通过 StartWorkflowOperation 创建子工作流实例
     * - 记录 subWorkflowId，并根据子工作流当前状态设置本任务状态
     */
    @SuppressWarnings("unchecked")
    @Override
    public void start(WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        Map<String, Object> input = task.getInputData();
        // 子工作流名称与版本
        String name = input.get("subWorkflowName").toString();
        int version = (int) input.get("subWorkflowVersion");

        WorkflowDef workflowDefinition = null;
        if (input.get("subWorkflowDefinition") != null) {
            // 内联子工作流定义：把它转换回 WorkflowDef 对象
            workflowDefinition =
                    objectMapper.convertValue(
                            input.get("subWorkflowDefinition"), WorkflowDef.class);
            name = workflowDefinition.getName();
        }

        // taskToDomain：默认继承父工作流的，若子工作流单独指定则覆盖
        Map<String, String> taskToDomain = workflow.getTaskToDomain();
        if (input.get("subWorkflowTaskToDomain") instanceof Map) {
            taskToDomain = (Map<String, String>) input.get("subWorkflowTaskToDomain");
        }

        // 子工作流输入：若未单独指定 workflowInput，则直接用父任务的 input
        var wfInput = (Map<String, Object>) input.get("workflowInput");
        if (wfInput == null || wfInput.isEmpty()) {
            wfInput = input;
        }
        String correlationId = workflow.getCorrelationId();

        try {
            // 组装启动子工作流的输入
            StartWorkflowInput startWorkflowInput = new StartWorkflowInput();
            startWorkflowInput.setWorkflowDefinition(workflowDefinition);
            startWorkflowInput.setName(name);
            startWorkflowInput.setVersion(version);
            startWorkflowInput.setWorkflowInput(wfInput);
            startWorkflowInput.setCorrelationId(correlationId);
            // 记录父子关系：父工作流 id + 父任务 id
            startWorkflowInput.setParentWorkflowId(workflow.getWorkflowId());
            startWorkflowInput.setParentWorkflowTaskId(task.getTaskId());
            startWorkflowInput.setTaskToDomain(taskToDomain);

            // 启动子工作流
            String subWorkflowId = startWorkflowOperation.execute(startWorkflowInput);

            task.setSubWorkflowId(subWorkflowId);
            // 向后兼容：同时写入输出字段
            task.addOutput(SUB_WORKFLOW_ID, subWorkflowId);

            // 根据子工作流当前状态设置本任务状态（递归期间状态可能已变化）
            WorkflowModel subWorkflow = workflowExecutor.getWorkflow(subWorkflowId, false);
            updateTaskStatus(subWorkflow, task);
        } catch (TransientException te) {
            // 临时性后端错误：记录日志，让引擎后续重试（不置为 FAILED）
            LOGGER.info(
                    "A transient backend error happened when task {} in {} tried to start sub workflow {}.",
                    task.getTaskId(),
                    workflow.toShortString(),
                    name);
        } catch (Exception ae) {
            // 其他异常：任务置为 FAILED
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion(ae.getMessage());
            LOGGER.error(
                    "Error starting sub workflow: {} from workflow: {}",
                    name,
                    workflow.toShortString(),
                    ae);
        }
    }

    /**
     * 检查子工作流是否已完成：
     * - 子工作流 id 为空 → 返回 false
     * - 子工作流未终态 → 返回 false
     * - 子工作流终态 → 更新本任务状态，返回 true
     *
     * 注意：由于 isAsyncComplete=true，正常情况下子工作流完成时会主动反向更新本任务，
     * 这里主要作为兜底检查。
     */
    @Override
    public boolean execute(
            WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        String workflowId = task.getSubWorkflowId();
        if (StringUtils.isEmpty(workflowId)) {
            return false;
        }

        WorkflowModel subWorkflow = workflowExecutor.getWorkflow(workflowId, false);
        WorkflowModel.Status subWorkflowStatus = subWorkflow.getStatus();
        if (!subWorkflowStatus.isTerminal()) {
            return false;
        }

        updateTaskStatus(subWorkflow, task);
        return true;
    }

    /**
     * 取消子工作流任务：终止对应的子工作流。
     * 终止原因取父工作流的终止原因（若有）。
     */
    @Override
    public void cancel(WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        String workflowId = task.getSubWorkflowId();
        if (StringUtils.isEmpty(workflowId)) {
            return;
        }
        WorkflowModel subWorkflow = workflowExecutor.getWorkflow(workflowId, true);
        subWorkflow.setStatus(WorkflowModel.Status.TERMINATED);
        String reason =
                StringUtils.isEmpty(workflow.getReasonForIncompletion())
                        ? "Parent workflow has been terminated with status " + workflow.getStatus()
                        : "Parent workflow has been terminated with reason: "
                        + workflow.getReasonForIncompletion();
        workflowExecutor.terminateWorkflow(subWorkflow, reason, null);
    }

    /** 异步系统任务：由引擎异步调度 */
    @Override
    public boolean isAsync() {
        return true;
    }

    /**
     * 保持 SUB_WORKFLOW 任务为 asyncComplete：
     * 该任务只会被异步执行一次进入 IN_PROGRESS，之后由子工作流的 completeWorkflow
     * 逻辑负责把它推进到终态，从而避免父工作流周期性轮询。
     */
    @Override
    public boolean isAsyncComplete(TaskModel task) {
        return true;
    }

    /**
     * 根据子工作流状态更新本任务状态，并在子工作流终态时同步输出与失败原因。
     *
     * 状态映射：
     * - RUNNING / PAUSED → IN_PROGRESS
     * - COMPLETED → COMPLETED
     * - FAILED → FAILED
     * - TERMINATED → CANCELED
     * - TIMED_OUT → TIMED_OUT
     */
    private void updateTaskStatus(WorkflowModel subworkflow, TaskModel task) {
        WorkflowModel.Status status = subworkflow.getStatus();
        switch (status) {
            case RUNNING:
            case PAUSED:
                task.setStatus(TaskModel.Status.IN_PROGRESS);
                break;
            case COMPLETED:
                task.setStatus(TaskModel.Status.COMPLETED);
                break;
            case FAILED:
                task.setStatus(TaskModel.Status.FAILED);
                break;
            case TERMINATED:
                task.setStatus(TaskModel.Status.CANCELED);
                break;
            case TIMED_OUT:
                task.setStatus(TaskModel.Status.TIMED_OUT);
                break;
            default:
                throw new NonTransientException(
                        "Subworkflow status does not conform to relevant task status.");
        }

        // 子工作流终态：同步输出与失败原因到父任务
        if (status.isTerminal()) {
            if (subworkflow.getExternalOutputPayloadStoragePath() != null) {
                // 输出存在外部存储：只记录路径
                task.setExternalOutputPayloadStoragePath(
                        subworkflow.getExternalOutputPayloadStoragePath());
            } else {
                // 否则把子工作流输出合并到父任务输出
                task.addOutput(subworkflow.getOutput());
            }
            if (!status.isSuccessful()) {
                task.setReasonForIncompletion(
                        String.format(
                                "Sub workflow %s failure reason: %s",
                                subworkflow.toShortString(),
                                subworkflow.getReasonForIncompletion()));
            }
        }
    }

    /**
     * 获取工作流数据时不需要检索任务。
     * 子工作流任务的完成由子工作流自身反向更新，不需要拉取任务列表，可提升性能。
     *
     * @return false
     */
    @Override
    public boolean isTaskRetrievalRequired() {
        return false;
    }
}