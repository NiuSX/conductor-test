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
package com.netflix.conductor.core.execution.mapper;

import java.util.Map;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.execution.DeciderService;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/** Business Object class used for interaction between the DeciderService and Different Mappers */
// 用于 DeciderService 与各种 Mapper 之间交互的业务对象类
public class TaskMapperContext {

    // 当前工作流运行时实例
    private final WorkflowModel workflowModel;
    // 任务定义（重试策略、超时等元数据）
    private final TaskDef taskDefinition;
    // 工作流定义中的任务节点（静态配置）
    private final WorkflowTask workflowTask;
    // 解析后的任务输入参数
    private final Map<String, Object> taskInput;
    // 当前重试次数
    private final int retryCount;
    // 被重试的原任务 ID
    private final String retryTaskId;
    // 本次要创建的任务 ID
    private final String taskId;
    // 调度服务，Mapper 在需要时可回调它获取更多信息
    private final DeciderService deciderService;

    // 私有构造，只能通过 Builder 创建
    private TaskMapperContext(Builder builder) {
        workflowModel = builder.workflowModel;
        taskDefinition = builder.taskDefinition;
        workflowTask = builder.workflowTask;
        taskInput = builder.taskInput;
        retryCount = builder.retryCount;
        retryTaskId = builder.retryTaskId;
        taskId = builder.taskId;
        deciderService = builder.deciderService;
    }

    // 创建一个空的 Builder
    public static Builder newBuilder() {
        return new Builder();
    }

    // 基于已有上下文创建一个 Builder，并复制其所有字段
    public static Builder newBuilder(TaskMapperContext copy) {
        Builder builder = new Builder();
        builder.workflowModel = copy.getWorkflowModel();
        builder.taskDefinition = copy.getTaskDefinition();
        builder.workflowTask = copy.getWorkflowTask();
        builder.taskInput = copy.getTaskInput();
        builder.retryCount = copy.getRetryCount();
        builder.retryTaskId = copy.getRetryTaskId();
        builder.taskId = copy.getTaskId();
        builder.deciderService = copy.getDeciderService();
        return builder;
    }

    // 获取当前工作流定义
    public WorkflowDef getWorkflowDefinition() {
        return workflowModel.getWorkflowDefinition();
    }

    // 获取当前工作流运行时实例
    public WorkflowModel getWorkflowModel() {
        return workflowModel;
    }

    // 获取任务定义
    public TaskDef getTaskDefinition() {
        return taskDefinition;
    }

    // 获取工作流定义中的任务节点
    public WorkflowTask getWorkflowTask() {
        return workflowTask;
    }

    // 获取当前重试次数
    public int getRetryCount() {
        return retryCount;
    }

    // 获取被重试的原任务 ID
    public String getRetryTaskId() {
        return retryTaskId;
    }

    // 获取本次要创建的任务 ID
    public String getTaskId() {
        return taskId;
    }

    // 获取任务输入参数
    public Map<String, Object> getTaskInput() {
        return taskInput;
    }

    // 获取调度服务
    public DeciderService getDeciderService() {
        return deciderService;
    }

    /**
     * 基于当前上下文创建一个 TaskModel 骨架实例。
     *
     * 这里只填充通用字段（引用名、工作流信息、调度时间、taskId 等），
     * 任务类型相关的属性（如 taskType、taskDefName）会被后续各 Mapper 实现覆盖。
     */
    public TaskModel createTaskModel() {
        TaskModel taskModel = new TaskModel();
        // 设置任务引用名
        taskModel.setReferenceTaskName(workflowTask.getTaskReferenceName());
        // 设置工作流实例 ID
        taskModel.setWorkflowInstanceId(workflowModel.getWorkflowId());
        // 设置工作流类型（名称）
        taskModel.setWorkflowType(workflowModel.getWorkflowName());
        // 设置关联 ID
        taskModel.setCorrelationId(workflowModel.getCorrelationId());
        // 设置调度时间
        taskModel.setScheduledTime(System.currentTimeMillis());

        // 设置任务 ID
        taskModel.setTaskId(taskId);
        // 绑定工作流任务定义
        taskModel.setWorkflowTask(workflowTask);
        // 设置工作流优先级
        taskModel.setWorkflowPriority(workflowModel.getPriority());

        // the following properties are overridden by some TaskMapper implementations
        // 以下属性会被某些 TaskMapper 实现覆盖
        taskModel.setTaskType(workflowTask.getType());
        taskModel.setTaskDefName(workflowTask.getName());
        return taskModel;
    }

    // 返回该上下文的字符串表示
    @Override
    public String toString() {
        return "TaskMapperContext{"
                + "workflowDefinition="
                + getWorkflowDefinition()
                + ", workflowModel="
                + workflowModel
                + ", workflowTask="
                + workflowTask
                + ", taskInput="
                + taskInput
                + ", retryCount="
                + retryCount
                + ", retryTaskId='"
                + retryTaskId
                + '\''
                + ", taskId='"
                + taskId
                + '\''
                + '}';
    }

    // 判断两个上下文是否相等
    @Override
    public boolean equals(Object o) {
        // 同一引用直接相等
        if (this == o) {
            return true;
        }
        // 类型不匹配则不相等
        if (!(o instanceof TaskMapperContext)) {
            return false;
        }

        TaskMapperContext that = (TaskMapperContext) o;

        // 比较重试次数
        if (getRetryCount() != that.getRetryCount()) {
            return false;
        }
        // 比较工作流定义
        if (!getWorkflowDefinition().equals(that.getWorkflowDefinition())) {
            return false;
        }
        // 比较工作流运行时实例
        if (!getWorkflowModel().equals(that.getWorkflowModel())) {
            return false;
        }
        // 比较任务节点
        if (!getWorkflowTask().equals(that.getWorkflowTask())) {
            return false;
        }
        // 比较任务输入
        if (!getTaskInput().equals(that.getTaskInput())) {
            return false;
        }
        // 比较被重试的原任务 ID（可能为 null）
        if (getRetryTaskId() != null
                ? !getRetryTaskId().equals(that.getRetryTaskId())
                : that.getRetryTaskId() != null) {
            return false;
        }
        // 比较任务 ID
        return getTaskId().equals(that.getTaskId());
    }

    // 计算哈希值
    @Override
    public int hashCode() {
        int result = getWorkflowDefinition().hashCode();
        result = 31 * result + getWorkflowModel().hashCode();
        result = 31 * result + getWorkflowTask().hashCode();
        result = 31 * result + getTaskInput().hashCode();
        result = 31 * result + getRetryCount();
        result = 31 * result + (getRetryTaskId() != null ? getRetryTaskId().hashCode() : 0);
        result = 31 * result + getTaskId().hashCode();
        return result;
    }

    /** {@code TaskMapperContext} builder static inner class. */
    // TaskMapperContext 的建造者（Builder）静态内部类
    public static final class Builder {

        // 当前工作流运行时实例
        private WorkflowModel workflowModel;
        // 任务定义
        private TaskDef taskDefinition;
        // 工作流定义中的任务节点
        private WorkflowTask workflowTask;
        // 任务输入参数
        private Map<String, Object> taskInput;
        // 当前重试次数
        private int retryCount;
        // 被重试的原任务 ID
        private String retryTaskId;
        // 本次要创建的任务 ID
        private String taskId;
        // 调度服务
        private DeciderService deciderService;

        // 私有构造，外部只能通过 newBuilder() 创建
        private Builder() {}

        /**
         * Sets the {@code workflowModel} and returns a reference to this Builder so that the
         * methods can be chained together.
         *
         * @param val the {@code workflowModel} to set
         * @return a reference to this Builder
         */
        // 设置 workflowModel 并返回当前 Builder，便于链式调用
        public Builder withWorkflowModel(WorkflowModel val) {
            workflowModel = val;
            return this;
        }

        /**
         * Sets the {@code taskDefinition} and returns a reference to this Builder so that the
         * methods can be chained together.
         *
         * @param val the {@code taskDefinition} to set
         * @return a reference to this Builder
         */
        // 设置 taskDefinition 并返回当前 Builder，便于链式调用
        public Builder withTaskDefinition(TaskDef val) {
            taskDefinition = val;
            return this;
        }

        /**
         * Sets the {@code workflowTask} and returns a reference to this Builder so that the methods
         * can be chained together.
         *
         * @param val the {@code workflowTask} to set
         * @return a reference to this Builder
         */
        // 设置 workflowTask 并返回当前 Builder，便于链式调用
        public Builder withWorkflowTask(WorkflowTask val) {
            workflowTask = val;
            return this;
        }

        /**
         * Sets the {@code taskInput} and returns a reference to this Builder so that the methods
         * can be chained together.
         *
         * @param val the {@code taskInput} to set
         * @return a reference to this Builder
         */
        // 设置 taskInput 并返回当前 Builder，便于链式调用
        public Builder withTaskInput(Map<String, Object> val) {
            taskInput = val;
            return this;
        }

        /**
         * Sets the {@code retryCount} and returns a reference to this Builder so that the methods
         * can be chained together.
         *
         * @param val the {@code retryCount} to set
         * @return a reference to this Builder
         */
        // 设置 retryCount 并返回当前 Builder，便于链式调用
        public Builder withRetryCount(int val) {
            retryCount = val;
            return this;
        }

        /**
         * Sets the {@code retryTaskId} and returns a reference to this Builder so that the methods
         * can be chained together.
         *
         * @param val the {@code retryTaskId} to set
         * @return a reference to this Builder
         */
        // 设置 retryTaskId 并返回当前 Builder，便于链式调用
        public Builder withRetryTaskId(String val) {
            retryTaskId = val;
            return this;
        }

        /**
         * Sets the {@code taskId} and returns a reference to this Builder so that the methods can
         * be chained together.
         *
         * @param val the {@code taskId} to set
         * @return a reference to this Builder
         */
        // 设置 taskId 并返回当前 Builder，便于链式调用
        public Builder withTaskId(String val) {
            taskId = val;
            return this;
        }

        /**
         * Sets the {@code deciderService} and returns a reference to this Builder so that the
         * methods can be chained together.
         *
         * @param val the {@code deciderService} to set
         * @return a reference to this Builder
         */
        // 设置 deciderService 并返回当前 Builder，便于链式调用
        public Builder withDeciderService(DeciderService val) {
            deciderService = val;
            return this;
        }

        /**
         * Returns a {@code TaskMapperContext} built from the parameters previously set.
         *
         * @return a {@code TaskMapperContext} built with parameters of this {@code
         *     TaskMapperContext.Builder}
         */
        // 基于已设置的参数构建一个 TaskMapperContext
        public TaskMapperContext build() {
            return new TaskMapperContext(this);
        }
    }
}