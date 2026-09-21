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

import java.util.Optional;

import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/**
 * 工作流系统任务的抽象基类。
 *
 * Conductor 的"系统任务"（如 Wait、Switch、Join、SubWorkflow、HTTP 等）都继承自这个类。
 * 它们由引擎内置处理，不需要外部 Worker。引擎通过 {@link SystemTaskRegistry} 按 taskType
 * 找到具体实现，并调用 start() / execute() / cancel() 等生命周期方法。
 *
 * 子类通常标注 @Component(TASK_TYPE_XXX)，由 Spring 自动收集进注册表。
 */
public abstract class WorkflowSystemTask {

    /** 任务类型（如 "WAIT"、"SWITCH"、"HTTP"），注册表的 key */
    private final String taskType;

    public WorkflowSystemTask(String taskType) {
        this.taskType = taskType;
    }

    /**
     * 启动任务执行。
     *
     * <p>仅在任务状态为 SCHEDULED 时被调用一次，且是生命周期中的第一个方法。
     * 默认空实现，子类按需覆盖。比如 HttpTask 在这里发起 HTTP 调用。
     *
     * @param workflow 任务所属的工作流
     * @param task 任务实例
     * @param workflowExecutor 工作流执行器
     */
    public void start(WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        // 默认不做任何事，由子类覆盖
    }

    /**
     * "执行"任务。
     *
     * <p>在 {@link #start(WorkflowModel, TaskModel, WorkflowExecutor)} 之后调用，
     * 前提是任务状态尚未终态。可以被多次调用（引擎会在调度周期里反复检查）。
     * 典型用途：Wait 任务在这里检查是否到达等待截止时间。
     *
     * @param workflow 任务所属的工作流
     * @param task 任务实例
     * @param workflowExecutor 工作流执行器
     * @return true 表示本次执行改变了任务状态；false 表示状态未变
     */
    public boolean execute(
            WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        return false;
    }

    /**
     * 取消任务执行。
     * 默认空实现，子类可覆盖做清理。比如 Wait 直接把状态置为 CANCELED。
     *
     * @param workflow 任务所属的工作流
     * @param task 任务实例
     * @param workflowExecutor 工作流执行器
     */
    public void cancel(WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {}

    /**
     * 获取该任务的评估偏移量（用于控制引擎多久后再次检查该任务）。
     * 默认返回 empty，表示使用引擎的默认调度周期。
     *
     * @param taskModel 任务模型
     * @param defaultOffset 默认偏移量
     * @return 自定义偏移量（若有）
     */
    public Optional<Long> getEvaluationOffset(TaskModel taskModel, long defaultOffset) {
        return Optional.empty();
    }

    /**
     * @return true 表示该任务应通过内部队列异步启动（由引擎后续调度，而非立即同步执行）。
     *         默认 false，即同步系统任务。
     */
    public boolean isAsync() {
        return false;
    }

    /**
     * 判断任务是否为"异步完成"：即任务保持 IN_PROGRESS 状态，等待外部消息再置为 COMPLETE。
     *
     * 判断依据（优先顺序）：
     * 1. 任务输入里是否有 asyncComplete 字段，有则取其布尔值
     * 2. 否则看 WorkflowTask 定义的 isAsyncComplete
     *
     * @param task 任务模型
     * @return true 表示异步完成
     */
    public boolean isAsyncComplete(TaskModel task) {
        if (task.getInputData().containsKey("asyncComplete")) {
            return Optional.ofNullable(task.getInputData().get("asyncComplete"))
                    .map(result -> (Boolean) result)
                    .orElse(false);
        } else {
            return Optional.ofNullable(task.getWorkflowTask())
                    .map(WorkflowTask::isAsyncComplete)
                    .orElse(false);
        }
    }

    /**
     * @return 系统任务的名称/类型
     */
    public String getTaskType() {
        return taskType;
    }

    /**
     * 获取工作流数据时是否需要一并检索任务。
     * 默认 true。某些场景（如子工作流）可能完全不需要任务数据，
     * 此时返回 false 可获得明显的性能提升。
     *
     * @return true 表示获取工作流时需要检索任务
     */
    public boolean isTaskRetrievalRequired() {
        return true;
    }

    @Override
    public String toString() {
        return taskType;
    }
}