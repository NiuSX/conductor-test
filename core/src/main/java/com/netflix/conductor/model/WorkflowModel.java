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
package com.netflix.conductor.model;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.utils.Utils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 工作流实例模型：表示一个正在运行/已结束的工作流实例（区别于 WorkflowDef 定义）。
 *
 * 这是 Conductor 执行引擎的核心数据模型，会被持久化到数据库。
 * 它包含：状态、任务列表、输入/输出、父子关系、超时/失败信息等。
 *
 * 注意：字段上的 @JsonIgnore 表示运行时内存对象不直接序列化该字段，
 * 而是通过 getRawInput / getRawOutput 等 @JsonProperty 方法做持久化。
 */
public class WorkflowModel {

    /**
     * 工作流状态枚举，每个状态自带两个标志：
     * - terminal：是否为终态（不可再变更）
     * - successful：是否为成功态
     */
    public enum Status {
        RUNNING(false, false),      // 运行中
        COMPLETED(true, true),      // 已完成（成功）
        FAILED(true, false),        // 失败
        TIMED_OUT(true, false),     // 超时
        TERMINATED(true, false),    // 被终止
        PAUSED(false, true);        // 暂停（非终态，但视为"成功"以便区分）

        private final boolean terminal;
        private final boolean successful;

        Status(boolean terminal, boolean successful) {
            this.terminal = terminal;
            this.successful = successful;
        }

        public boolean isTerminal() {
            return terminal;
        }

        public boolean isSuccessful() {
            return successful;
        }
    }

    /** 当前状态，默认 RUNNING */
    private Status status = Status.RUNNING;

    /** 结束时间（毫秒时间戳） */
    private long endTime;

    /** 工作流实例 id（全局唯一） */
    private String workflowId;

    /** 父工作流 id（若是子工作流） */
    private String parentWorkflowId;

    /** 父工作流中对应本子工作流的任务 id */
    private String parentWorkflowTaskId;

    /** 任务列表（按执行顺序） */
    private List<TaskModel> tasks = new LinkedList<>();

    /** 关联 id，用于把同一业务链路的多个工作流关联起来 */
    private String correlationId;

    /** 若是重跑，记录从哪个工作流 id 重跑 */
    private String reRunFromWorkflowId;

    /** 未完成/失败的原因 */
    private String reasonForIncompletion;

    /** 事件名（用于事件触发的工作流） */
    private String event;

    /** 任务类型 → domain 的映射，用于任务路由 */
    private Map<String, String> taskToDomain = new HashMap<>();

    /** 失败的引用任务名集合（仅非空时序列化） */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Set<String> failedReferenceTaskNames = new HashSet<>();

    /** 失败的任务定义名集合（仅非空时序列化） */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Set<String> failedTaskNames = new HashSet<>();

    /** 工作流定义（启动时固定的快照） */
    private WorkflowDef workflowDefinition;

    /** 输入被外部化存储时的路径（大输入存对象存储） */
    private String externalInputPayloadStoragePath;

    /** 输出被外部化存储时的路径（大输出存对象存储） */
    private String externalOutputPayloadStoragePath;

    /** 优先级（0~99） */
    private int priority;

    /** 工作流变量（运行时可读写） */
    private Map<String, Object> variables = new HashMap<>();

    /** 上次重试时间（用于超时计算基准） */
    private long lastRetriedTime;

    /** 所属应用 */
    private String ownerApp;

    /** 创建时间 */
    private Long createTime;

    /** 更新时间 */
    private Long updatedTime;

    /** 创建者 */
    private String createdBy;

    /** 更新者 */
    private String updatedBy;

    // 若工作流因任务失败而失败，记录失败的任务 id
    private String failedTaskId;

    /** 前一个状态（用于状态变更追踪） */
    private Status previousStatus;

    /** 输入数据（内存态，不直接序列化） */
    @JsonIgnore private Map<String, Object> input = new HashMap<>();

    /** 输出数据（内存态，不直接序列化） */
    @JsonIgnore private Map<String, Object> output = new HashMap<>();

    /** 外部化后的输入数据（内存态） */
    @JsonIgnore private Map<String, Object> inputPayload = new HashMap<>();

    /** 外部化后的输出数据（内存态） */
    @JsonIgnore private Map<String, Object> outputPayload = new HashMap<>();

    public Status getPreviousStatus() {
        return previousStatus;
    }

    public void setPreviousStatus(Status status) {
        this.previousStatus = status;
    }

    public Status getStatus() {
        return status;
    }

    /** 设置状态时自动记录前一个状态（仅当状态确实变化时） */
    public void setStatus(Status status) {
        // 状态变化时更新 previousStatus
        if (this.status != status) {
            setPreviousStatus(this.status);
        }
        this.status = status;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public String getParentWorkflowId() {
        return parentWorkflowId;
    }

    public void setParentWorkflowId(String parentWorkflowId) {
        this.parentWorkflowId = parentWorkflowId;
    }

    public String getParentWorkflowTaskId() {
        return parentWorkflowTaskId;
    }

    public void setParentWorkflowTaskId(String parentWorkflowTaskId) {
        this.parentWorkflowTaskId = parentWorkflowTaskId;
    }

    public List<TaskModel> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskModel> tasks) {
        this.tasks = tasks;
    }

    /**
     * 获取输入：合并 input 与 inputPayload。
     * 若两者都有数据，合并后清空 inputPayload；否则返回非空的那个。
     */
    @JsonIgnore
    public Map<String, Object> getInput() {
        if (!inputPayload.isEmpty() && !input.isEmpty()) {
            input.putAll(inputPayload);
            inputPayload = new HashMap<>();
            return input;
        } else if (inputPayload.isEmpty()) {
            return input;
        } else {
            return inputPayload;
        }
    }

    @JsonIgnore
    public void setInput(Map<String, Object> input) {
        if (input == null) {
            input = new HashMap<>();
        }
        this.input = input;
    }

    /** 获取输出：合并 output 与 outputPayload，逻辑同 getInput */
    @JsonIgnore
    public Map<String, Object> getOutput() {
        if (!outputPayload.isEmpty() && !output.isEmpty()) {
            output.putAll(outputPayload);
            outputPayload = new HashMap<>();
            return output;
        } else if (outputPayload.isEmpty()) {
            return output;
        } else {
            return outputPayload;
        }
    }

    @JsonIgnore
    public void setOutput(Map<String, Object> output) {
        if (output == null) {
            output = new HashMap<>();
        }
        this.output = output;
    }

    /**
     * 仅用于 JSON 序列化/反序列化的原始输入访问器。
     * @deprecated 运行时请使用 getInput()
     */
    @Deprecated
    @JsonProperty("input")
    public Map<String, Object> getRawInput() {
        return input;
    }

    /**
     * 仅用于 JSON 序列化/反序列化的原始输入设置器。
     * @deprecated 运行时请使用 setInput()
     */
    @Deprecated
    @JsonProperty("input")
    public void setRawInput(Map<String, Object> input) {
        setInput(input);
    }

    /**
     * 仅用于 JSON 序列化/反序列化的原始输出访问器。
     * @deprecated 运行时请使用 getOutput()
     */
    @Deprecated
    @JsonProperty("output")
    public Map<String, Object> getRawOutput() {
        return output;
    }

    /**
     * 仅用于 JSON 序列化/反序列化的原始输出设置器。
     * @deprecated 运行时请使用 setOutput()
     */
    @Deprecated
    @JsonProperty("output")
    public void setRawOutput(Map<String, Object> output) {
        setOutput(output);
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getReRunFromWorkflowId() {
        return reRunFromWorkflowId;
    }

    public void setReRunFromWorkflowId(String reRunFromWorkflowId) {
        this.reRunFromWorkflowId = reRunFromWorkflowId;
    }

    public String getReasonForIncompletion() {
        return reasonForIncompletion;
    }

    public void setReasonForIncompletion(String reasonForIncompletion) {
        this.reasonForIncompletion = reasonForIncompletion;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Map<String, String> getTaskToDomain() {
        return taskToDomain;
    }

    public void setTaskToDomain(Map<String, String> taskToDomain) {
        this.taskToDomain = taskToDomain;
    }

    public Set<String> getFailedReferenceTaskNames() {
        return failedReferenceTaskNames;
    }

    public void setFailedReferenceTaskNames(Set<String> failedReferenceTaskNames) {
        this.failedReferenceTaskNames = failedReferenceTaskNames;
    }

    public Set<String> getFailedTaskNames() {
        return failedTaskNames;
    }

    public void setFailedTaskNames(Set<String> failedTaskNames) {
        this.failedTaskNames = failedTaskNames;
    }

    public WorkflowDef getWorkflowDefinition() {
        return workflowDefinition;
    }

    public void setWorkflowDefinition(WorkflowDef workflowDefinition) {
        this.workflowDefinition = workflowDefinition;
    }

    public String getExternalInputPayloadStoragePath() {
        return externalInputPayloadStoragePath;
    }

    public void setExternalInputPayloadStoragePath(String externalInputPayloadStoragePath) {
        this.externalInputPayloadStoragePath = externalInputPayloadStoragePath;
    }

    public String getExternalOutputPayloadStoragePath() {
        return externalOutputPayloadStoragePath;
    }

    public void setExternalOutputPayloadStoragePath(String externalOutputPayloadStoragePath) {
        this.externalOutputPayloadStoragePath = externalOutputPayloadStoragePath;
    }

    public int getPriority() {
        return priority;
    }

    /** 设置优先级，必须在 0~99 之间 */
    public void setPriority(int priority) {
        if (priority < 0 || priority > 99) {
            throw new IllegalArgumentException("priority MUST be between 0 and 99 (inclusive)");
        }
        this.priority = priority;
    }

    public Map<String, Object> getVariables() {
        return variables;
    }

    public void setVariables(Map<String, Object> variables) {
        this.variables = variables;
    }

    public long getLastRetriedTime() {
        return lastRetriedTime;
    }

    public void setLastRetriedTime(long lastRetriedTime) {
        this.lastRetriedTime = lastRetriedTime;
    }

    public String getOwnerApp() {
        return ownerApp;
    }

    public void setOwnerApp(String ownerApp) {
        this.ownerApp = ownerApp;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public Long getUpdatedTime() {
        return updatedTime;
    }

    public void setUpdatedTime(Long updatedTime) {
        this.updatedTime = updatedTime;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public String getFailedTaskId() {
        return failedTaskId;
    }

    public void setFailedTaskId(String failedTaskId) {
        this.failedTaskId = failedTaskId;
    }

    /**
     * 便捷方法：获取工作流定义名。
     * @return 工作流定义名
     */
    public String getWorkflowName() {
        Utils.checkNotNull(workflowDefinition, "Workflow definition is null");
        return workflowDefinition.getName();
    }

    /**
     * 便捷方法：获取工作流定义版本。
     * @return 工作流定义版本
     */
    public int getWorkflowVersion() {
        Utils.checkNotNull(workflowDefinition, "Workflow definition is null");
        return workflowDefinition.getVersion();
    }

    /** 是否为子工作流（有父工作流 id） */
    public boolean hasParent() {
        return StringUtils.isNotEmpty(parentWorkflowId);
    }

    /**
     * 返回能标识该工作流的简短字符串，用于日志和系统消息。
     */
    public String toShortString() {
        String name = workflowDefinition != null ? workflowDefinition.getName() : null;
        Integer version = workflowDefinition != null ? workflowDefinition.getVersion() : null;
        return String.format("%s.%s/%s", name, version, workflowId);
    }

    /**
     * 按引用名查找任务，返回最后一个匹配的任务（因为同一引用名可能因重试/迭代出现多次）。
     *
     * @param refName 任务引用名
     * @return 匹配的任务；找不到返回 null
     */
    public TaskModel getTaskByRefName(String refName) {
        if (refName == null) {
            throw new RuntimeException(
                    "refName passed is null.  Check the workflow execution.  For dynamic tasks, make sure referenceTaskName is set to a not null value");
        }
        LinkedList<TaskModel> found = new LinkedList<>();
        for (TaskModel task : tasks) {
            if (task.getReferenceTaskName() == null) {
                throw new RuntimeException(
                        "Task "
                                + task.getTaskDefName()
                                + ", seq="
                                + task.getSeq()
                                + " does not have reference name specified.");
            }
            if (task.getReferenceTaskName().equals(refName)) {
                found.add(task);
            }
        }
        if (found.isEmpty()) {
            return null;
        }
        return found.getLast();
    }

    /**
     * 把输入外部化：把 input 移到 inputPayload，并记录外部存储路径。
     * 用于大输入存对象存储，避免占用数据库。
     */
    public void externalizeInput(String path) {
        this.inputPayload = this.input;
        this.input = new HashMap<>();
        this.externalInputPayloadStoragePath = path;
    }

    /** 把输出外部化，逻辑同 externalizeInput */
    public void externalizeOutput(String path) {
        this.outputPayload = this.output;
        this.output = new HashMap<>();
        this.externalOutputPayloadStoragePath = path;
    }

    /** 从外部存储加载输入：把数据放入 inputPayload */
    public void internalizeInput(Map<String, Object> data) {
        this.input = new HashMap<>();
        this.inputPayload = data;
    }

    /** 从外部存储加载输出：把数据放入 outputPayload */
    public void internalizeOutput(Map<String, Object> data) {
        this.output = new HashMap<>();
        this.outputPayload = data;
    }

    @Override
    public String toString() {
        String name = workflowDefinition != null ? workflowDefinition.getName() : null;
        Integer version = workflowDefinition != null ? workflowDefinition.getVersion() : null;
        return String.format("%s.%s/%s.%s", name, version, workflowId, status);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkflowModel that = (WorkflowModel) o;
        return getEndTime() == that.getEndTime()
                && getPriority() == that.getPriority()
                && getLastRetriedTime() == that.getLastRetriedTime()
                && getStatus() == that.getStatus()
                && Objects.equals(getWorkflowId(), that.getWorkflowId())
                && Objects.equals(getParentWorkflowId(), that.getParentWorkflowId())
                && Objects.equals(getParentWorkflowTaskId(), that.getParentWorkflowTaskId())
                && Objects.equals(getTasks(), that.getTasks())
                && Objects.equals(getInput(), that.getInput())
                && Objects.equals(output, that.output)
                && Objects.equals(outputPayload, that.outputPayload)
                && Objects.equals(getCorrelationId(), that.getCorrelationId())
                && Objects.equals(getReRunFromWorkflowId(), that.getReRunFromWorkflowId())
                && Objects.equals(getReasonForIncompletion(), that.getReasonForIncompletion())
                && Objects.equals(getEvent(), that.getEvent())
                && Objects.equals(getTaskToDomain(), that.getTaskToDomain())
                && Objects.equals(getFailedReferenceTaskNames(), that.getFailedReferenceTaskNames())
                && Objects.equals(getFailedTaskNames(), that.getFailedTaskNames())
                && Objects.equals(getWorkflowDefinition(), that.getWorkflowDefinition())
                && Objects.equals(
                getExternalInputPayloadStoragePath(),
                that.getExternalInputPayloadStoragePath())
                && Objects.equals(
                getExternalOutputPayloadStoragePath(),
                that.getExternalOutputPayloadStoragePath())
                && Objects.equals(getVariables(), that.getVariables())
                && Objects.equals(getOwnerApp(), that.getOwnerApp())
                && Objects.equals(getCreateTime(), that.getCreateTime())
                && Objects.equals(getUpdatedTime(), that.getUpdatedTime())
                && Objects.equals(getCreatedBy(), that.getCreatedBy())
                && Objects.equals(getUpdatedBy(), that.getUpdatedBy());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                getStatus(),
                getEndTime(),
                getWorkflowId(),
                getParentWorkflowId(),
                getParentWorkflowTaskId(),
                getTasks(),
                getInput(),
                output,
                outputPayload,
                getCorrelationId(),
                getReRunFromWorkflowId(),
                getReasonForIncompletion(),
                getEvent(),
                getTaskToDomain(),
                getFailedReferenceTaskNames(),
                getFailedTaskNames(),
                getWorkflowDefinition(),
                getExternalInputPayloadStoragePath(),
                getExternalOutputPayloadStoragePath(),
                getPriority(),
                getVariables(),
                getLastRetriedTime(),
                getOwnerApp(),
                getCreateTime(),
                getUpdatedTime(),
                getCreatedBy(),
                getUpdatedBy());
    }

    /**
     * 转换为对外 API 的 Workflow 对象（用于返回给客户端）。
     * 会复制属性、转换状态枚举、转换任务列表。
     */
    public Workflow toWorkflow() {
        Workflow workflow = new Workflow();
        BeanUtils.copyProperties(this, workflow);
        workflow.setStatus(Workflow.WorkflowStatus.valueOf(this.status.name()));
        workflow.setTasks(tasks.stream().map(TaskModel::toTask).collect(Collectors.toList()));
        workflow.setUpdateTime(this.updatedTime);

        // 若输入/输出被外部化，对外返回空 map（避免误导）
        if (externalInputPayloadStoragePath != null) {
            workflow.setInput(new HashMap<>());
        }
        if (externalOutputPayloadStoragePath != null) {
            workflow.setOutput(new HashMap<>());
        }
        return workflow;
    }

    /** 按 key 添加输入 */
    public void addInput(String key, Object value) {
        this.input.put(key, value);
    }

    /** 批量添加输入 */
    public void addInput(Map<String, Object> inputData) {
        if (inputData != null) {
            this.input.putAll(inputData);
        }
    }

    /** 按 key 添加输出 */
    public void addOutput(String key, Object value) {
        this.output.put(key, value);
    }

    /** 批量添加输出 */
    public void addOutput(Map<String, Object> outputData) {
        if (outputData != null) {
            this.output.putAll(outputData);
        }
    }
}