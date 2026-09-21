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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.Any;

/**
 * 任务实例模型：表示工作流中一个具体任务的运行时状态（区别于 TaskDef 定义）。
 *
 * 这是 Conductor 持久化执行的核心数据模型之一，每个任务的状态、输入输出、
 * 重试信息、超时时间等都会被持久化，从而支持故障恢复。
 *
 * 与 WorkflowModel 的关系：一个 WorkflowModel 包含多个 TaskModel。
 */
public class TaskModel {

    /**
     * 任务状态枚举，每个状态自带三个标志：
     * - terminal：是否为终态
     * - successful：是否为成功态
     * - retriable：是否可重试
     */
    public enum Status {
        IN_PROGRESS(false, true, true),          // 进行中
        CANCELED(true, false, false),            // 被取消（不可重试）
        FAILED(true, false, true),               // 失败（可重试）
        FAILED_WITH_TERMINAL_ERROR(true, false, false), // 失败且不可重试
        COMPLETED(true, true, true),             // 成功完成
        COMPLETED_WITH_ERRORS(true, true, true), // 完成但有错误
        SCHEDULED(false, true, true),            // 已调度
        TIMED_OUT(true, false, true),            // 超时（可重试）
        SKIPPED(true, true, false);              // 被跳过

        private final boolean terminal;
        private final boolean successful;
        private final boolean retriable;

        Status(boolean terminal, boolean successful, boolean retriable) {
            this.terminal = terminal;
            this.successful = successful;
            this.retriable = retriable;
        }

        public boolean isTerminal() {
            return terminal;
        }

        public boolean isSuccessful() {
            return successful;
        }

        public boolean isRetriable() {
            return retriable;
        }
    }

    /** 任务类型（如 "SIMPLE"、"WAIT"、"SWITCH"） */
    private String taskType;

    /** 当前状态 */
    private Status status;

    /** 任务引用名（在工作流内唯一，用于定位任务） */
    private String referenceTaskName;

    /** 重试次数 */
    private int retryCount;

    /** 序号（在工作流内的执行顺序） */
    private int seq;

    /** 关联 id */
    private String correlationId;

    /** 被 poll 的次数（用于计算评估偏移、退避） */
    private int pollCount;

    /** 任务定义名 */
    private String taskDefName;

    /** 任务被调度的时间 */
    private long scheduledTime;

    /** 任务首次被 poll 的时间 */
    private long startTime;

    /** 任务完成执行的时间 */
    private long endTime;

    /** 任务最后一次更新的时间 */
    private long updateTime;

    /** 启动延迟（秒），用于重试退避 */
    private int startDelayInSeconds;

    /** 被重试的原任务 id */
    private String retriedTaskId;

    /** 是否已被重试 */
    private boolean retried;

    /** 是否已执行完毕（生命周期结束） */
    private boolean executed;

    /** 是否由 Worker 回调（默认 true） */
    private boolean callbackFromWorker = true;

    /** 响应超时时间（秒） */
    private long responseTimeoutSeconds;

    /** 所属工作流实例 id */
    private String workflowInstanceId;

    /** 所属工作流类型（名称） */
    private String workflowType;

    /** 任务 id（全局唯一） */
    private String taskId;

    /** 未完成/失败的原因 */
    private String reasonForIncompletion;

    /** 延迟回调时间（秒），用于 postpone 任务 */
    private long callbackAfterSeconds;

    /** 执行该任务的 Worker id */
    private String workerId;

    /** 对应的 WorkflowTask 定义 */
    private WorkflowTask workflowTask;

    /** 目标 domain（用于任务路由） */
    private String domain;

    /** 输入消息（protobuf Any） */
    private Any inputMessage;

    /** 输出消息（protobuf Any） */
    private Any outputMessage;

    /** 频率限制：每频率内允许的任务数 */
    private int rateLimitPerFrequency;

    /** 频率限制：频率窗口（秒） */
    private int rateLimitFrequencyInSeconds;

    /** 输入被外部化存储时的路径 */
    private String externalInputPayloadStoragePath;

    /** 输出被外部化存储时的路径 */
    private String externalOutputPayloadStoragePath;

    /** 工作流优先级 */
    private int workflowPriority;

    /** 执行命名空间（隔离用） */
    private String executionNameSpace;

    /** 隔离组 id（隔离用） */
    private String isolationGroupId;

    /** 迭代号（循环任务用，>0 表示处于循环中） */
    private int iteration;

    /** 子工作流 id（SUB_WORKFLOW 任务用） */
    private String subWorkflowId;

    /** WAIT 任务的等待截止时间 */
    private long waitTimeout;

    /**
     * 标记 SUB_WORKFLOW 任务关联的子工作流是否被直接操作过（如重试/重启），
     * 用于通知父工作流重新评估。
     */
    private boolean subworkflowChanged;

    /** 外部化后的输入数据（内存态） */
    @JsonIgnore private Map<String, Object> inputPayload = new HashMap<>();

    /** 外部化后的输出数据（内存态） */
    @JsonIgnore private Map<String, Object> outputPayload = new HashMap<>();

    /** 输入数据（内存态，不直接序列化） */
    @JsonIgnore private Map<String, Object> inputData = new HashMap<>();

    /** 输出数据（内存态，不直接序列化） */
    @JsonIgnore private Map<String, Object> outputData = new HashMap<>();

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * 获取输入数据：合并 inputData 与 inputPayload。
     * 若两者都有数据，合并后清空 inputPayload；否则返回非空的那个。
     */
    @JsonIgnore
    public Map<String, Object> getInputData() {
        if (!inputPayload.isEmpty() && !inputData.isEmpty()) {
            inputData.putAll(inputPayload);
            inputPayload = new HashMap<>();
            return inputData;
        } else if (inputPayload.isEmpty()) {
            return inputData;
        } else {
            return inputPayload;
        }
    }

    @JsonIgnore
    public void setInputData(Map<String, Object> inputData) {
        if (inputData == null) {
            inputData = new HashMap<>();
        }
        this.inputData = inputData;
    }

    /**
     * 仅用于 JSON 序列化/反序列化的原始输入设置器。
     * @deprecated 运行时请使用 setInputData()
     */
    @JsonProperty("inputData")
    @Deprecated
    public void setRawInputData(Map<String, Object> inputData) {
        setInputData(inputData);
    }

    /**
     * 仅用于 JSON 序列化/反序列化的原始输入访问器。
     * @deprecated 运行时请使用 getInputData()
     */
    @JsonProperty("inputData")
    @Deprecated
    public Map<String, Object> getRawInputData() {
        return inputData;
    }

    public String getReferenceTaskName() {
        return referenceTaskName;
    }

    public void setReferenceTaskName(String referenceTaskName) {
        this.referenceTaskName = referenceTaskName;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getSeq() {
        return seq;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public int getPollCount() {
        return pollCount;
    }

    public void setPollCount(int pollCount) {
        this.pollCount = pollCount;
    }

    /** 获取任务定义名，若为空则回退到 taskType */
    public String getTaskDefName() {
        if (taskDefName == null || "".equals(taskDefName)) {
            taskDefName = taskType;
        }
        return taskDefName;
    }

    public void setTaskDefName(String taskDefName) {
        this.taskDefName = taskDefName;
    }

    public long getScheduledTime() {
        return scheduledTime;
    }

    public void setScheduledTime(long scheduledTime) {
        this.scheduledTime = scheduledTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public int getStartDelayInSeconds() {
        return startDelayInSeconds;
    }

    public void setStartDelayInSeconds(int startDelayInSeconds) {
        this.startDelayInSeconds = startDelayInSeconds;
    }

    public String getRetriedTaskId() {
        return retriedTaskId;
    }

    public void setRetriedTaskId(String retriedTaskId) {
        this.retriedTaskId = retriedTaskId;
    }

    public boolean isRetried() {
        return retried;
    }

    public void setRetried(boolean retried) {
        this.retried = retried;
    }

    public boolean isExecuted() {
        return executed;
    }

    public void setExecuted(boolean executed) {
        this.executed = executed;
    }

    public boolean isCallbackFromWorker() {
        return callbackFromWorker;
    }

    public void setCallbackFromWorker(boolean callbackFromWorker) {
        this.callbackFromWorker = callbackFromWorker;
    }

    public long getResponseTimeoutSeconds() {
        return responseTimeoutSeconds;
    }

    public void setResponseTimeoutSeconds(long responseTimeoutSeconds) {
        this.responseTimeoutSeconds = responseTimeoutSeconds;
    }

    public String getWorkflowInstanceId() {
        return workflowInstanceId;
    }

    public void setWorkflowInstanceId(String workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
    }

    public String getWorkflowType() {
        return workflowType;
    }

    public void setWorkflowType(String workflowType) {
        this.workflowType = workflowType;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getReasonForIncompletion() {
        return reasonForIncompletion;
    }

    public void setReasonForIncompletion(String reasonForIncompletion) {
        this.reasonForIncompletion = reasonForIncompletion;
    }

    public long getCallbackAfterSeconds() {
        return callbackAfterSeconds;
    }

    public void setCallbackAfterSeconds(long callbackAfterSeconds) {
        this.callbackAfterSeconds = callbackAfterSeconds;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    /**
     * 获取输出数据：合并 outputData 与 outputPayload。
     * 注意：outputData 优先于 outputPayload（因为外部存储场景下，
     * payload 是旧值，data 是最新值，若 payload 优先会覆盖最新输出）。
     */
    @JsonIgnore
    public Map<String, Object> getOutputData() {
        if (!outputPayload.isEmpty() && !outputData.isEmpty()) {
            // 合并 payload + data，data 优先于 payload
            outputPayload.forEach(outputData::putIfAbsent);
            outputPayload = new HashMap<>();
            return outputData;
        } else if (outputPayload.isEmpty()) {
            return outputData;
        } else {
            return outputPayload;
        }
    }

    @JsonIgnore
    public void setOutputData(Map<String, Object> outputData) {
        if (outputData == null) {
            outputData = new HashMap<>();
        }
        this.outputData = outputData;
    }

    /**
     * 仅用于 JSON 序列化/反序列化的原始输出设置器。
     * @deprecated 运行时请使用 setOutputData()
     */
    @JsonProperty("outputData")
    @Deprecated
    public void setRawOutputData(Map<String, Object> inputData) {
        setOutputData(inputData);
    }

    /**
     * 仅用于 JSON 序列化/反序列化的原始输出访问器。
     * @deprecated 运行时请使用 getOutputData()
     */
    @JsonProperty("outputData")
    @Deprecated
    public Map<String, Object> getRawOutputData() {
        return outputData;
    }

    public WorkflowTask getWorkflowTask() {
        return workflowTask;
    }

    public void setWorkflowTask(WorkflowTask workflowTask) {
        this.workflowTask = workflowTask;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public Any getInputMessage() {
        return inputMessage;
    }

    public void setInputMessage(Any inputMessage) {
        this.inputMessage = inputMessage;
    }

    public Any getOutputMessage() {
        return outputMessage;
    }

    public void setOutputMessage(Any outputMessage) {
        this.outputMessage = outputMessage;
    }

    public int getRateLimitPerFrequency() {
        return rateLimitPerFrequency;
    }

    public void setRateLimitPerFrequency(int rateLimitPerFrequency) {
        this.rateLimitPerFrequency = rateLimitPerFrequency;
    }

    public int getRateLimitFrequencyInSeconds() {
        return rateLimitFrequencyInSeconds;
    }

    public void setRateLimitFrequencyInSeconds(int rateLimitFrequencyInSeconds) {
        this.rateLimitFrequencyInSeconds = rateLimitFrequencyInSeconds;
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

    public int getWorkflowPriority() {
        return workflowPriority;
    }

    public void setWorkflowPriority(int workflowPriority) {
        this.workflowPriority = workflowPriority;
    }

    public String getExecutionNameSpace() {
        return executionNameSpace;
    }

    public void setExecutionNameSpace(String executionNameSpace) {
        this.executionNameSpace = executionNameSpace;
    }

    public String getIsolationGroupId() {
        return isolationGroupId;
    }

    public void setIsolationGroupId(String isolationGroupId) {
        this.isolationGroupId = isolationGroupId;
    }

    public int getIteration() {
        return iteration;
    }

    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    /**
     * 获取子工作流 id。
     * 向后兼容：若字段为空，则尝试从 outputData / inputData 中取 "subWorkflowId"。
     */
    public String getSubWorkflowId() {
        // 向后兼容
        if (StringUtils.isNotBlank(subWorkflowId)) {
            return subWorkflowId;
        } else {
            return this.getOutputData() != null && this.getOutputData().get("subWorkflowId") != null
                    ? (String) this.getOutputData().get("subWorkflowId")
                    : this.getInputData() != null
                    ? (String) this.getInputData().get("subWorkflowId")
                    : null;
        }
    }

    /** 设置子工作流 id，并同步更新 outputData 中的兼容字段 */
    public void setSubWorkflowId(String subWorkflowId) {
        this.subWorkflowId = subWorkflowId;
        // 向后兼容
        if (this.outputData != null && this.outputData.containsKey("subWorkflowId")) {
            this.outputData.put("subWorkflowId", subWorkflowId);
        }
    }

    public boolean isSubworkflowChanged() {
        return subworkflowChanged;
    }

    public void setSubworkflowChanged(boolean subworkflowChanged) {
        this.subworkflowChanged = subworkflowChanged;
    }

    /** 轮询次数 +1 */
    public void incrementPollCount() {
        ++this.pollCount;
    }

    /**
     * @return 任务定义（若存在）
     */
    public Optional<TaskDef> getTaskDefinition() {
        return Optional.ofNullable(this.getWorkflowTask()).map(WorkflowTask::getTaskDefinition);
    }

    /** 是否为循环任务（iteration > 0） */
    public boolean isLoopOverTask() {
        return iteration > 0;
    }

    public long getWaitTimeout() {
        return waitTimeout;
    }

    public void setWaitTimeout(long waitTimeout) {
        this.waitTimeout = waitTimeout;
    }

    /**
     * 计算任务在队列中的等待时间。
     * - 若已 start 且有 updateTime + callbackAfterSeconds，则按当前时间计算
     * - 否则用 startTime - scheduledTime
     *
     * @return 队列等待时间（毫秒）
     */
    public long getQueueWaitTime() {
        if (this.startTime > 0 && this.scheduledTime > 0) {
            if (this.updateTime > 0 && getCallbackAfterSeconds() > 0) {
                long waitTime =
                        System.currentTimeMillis()
                                - (this.updateTime + (getCallbackAfterSeconds() * 1000));
                return waitTime > 0 ? waitTime : 0;
            } else {
                return this.startTime - this.scheduledTime;
            }
        }
        return 0L;
    }

    /**
     * @return 任务实例的副本
     */
    public TaskModel copy() {
        TaskModel copy = new TaskModel();
        BeanUtils.copyProperties(this, copy);
        return copy;
    }

    /**
     * 把输入外部化：把 inputData 移到 inputPayload，并记录外部存储路径。
     * 用于大输入存对象存储，避免占用数据库。
     */
    public void externalizeInput(String path) {
        this.inputPayload = this.inputData;
        this.inputData = new HashMap<>();
        this.externalInputPayloadStoragePath = path;
    }

    /** 把输出外部化，逻辑同 externalizeInput */
    public void externalizeOutput(String path) {
        this.outputPayload = this.outputData;
        this.outputData = new HashMap<>();
        this.externalOutputPayloadStoragePath = path;
    }

    /** 从外部存储加载输入：把数据放入 inputPayload */
    public void internalizeInput(Map<String, Object> data) {
        this.inputData = new HashMap<>();
        this.inputPayload = data;
    }

    /** 从外部存储加载输出：把数据放入 outputPayload */
    public void internalizeOutput(Map<String, Object> data) {
        this.outputData = new HashMap<>();
        this.outputPayload = data;
    }

    @Override
    public String toString() {
        return "TaskModel{"
                + "taskType='"
                + taskType
                + '\''
                + ", status="
                + status
                + ", inputData="
                + inputData
                + ", referenceTaskName='"
                + referenceTaskName
                + '\''
                + ", retryCount="
                + retryCount
                + ", seq="
                + seq
                + ", correlationId='"
                + correlationId
                + '\''
                + ", pollCount="
                + pollCount
                + ", taskDefName='"
                + taskDefName
                + '\''
                + ", scheduledTime="
                + scheduledTime
                + ", startTime="
                + startTime
                + ", endTime="
                + endTime
                + ", updateTime="
                + updateTime
                + ", startDelayInSeconds="
                + startDelayInSeconds
                + ", retriedTaskId='"
                + retriedTaskId
                + '\''
                + ", retried="
                + retried
                + ", executed="
                + executed
                + ", callbackFromWorker="
                + callbackFromWorker
                + ", responseTimeoutSeconds="
                + responseTimeoutSeconds
                + ", workflowInstanceId='"
                + workflowInstanceId
                + '\''
                + ", workflowType='"
                + workflowType
                + '\''
                + ", taskId='"
                + taskId
                + '\''
                + ", reasonForIncompletion='"
                + reasonForIncompletion
                + '\''
                + ", callbackAfterSeconds="
                + callbackAfterSeconds
                + ", workerId='"
                + workerId
                + '\''
                + ", outputData="
                + outputData
                + ", workflowTask="
                + workflowTask
                + ", domain='"
                + domain
                + '\''
                + ", waitTimeout='"
                + waitTimeout
                + '\''
                + ", inputMessage="
                + inputMessage
                + ", outputMessage="
                + outputMessage
                + ", rateLimitPerFrequency="
                + rateLimitPerFrequency
                + ", rateLimitFrequencyInSeconds="
                + rateLimitFrequencyInSeconds
                + ", externalInputPayloadStoragePath='"
                + externalInputPayloadStoragePath
                + '\''
                + ", externalOutputPayloadStoragePath='"
                + externalOutputPayloadStoragePath
                + '\''
                + ", workflowPriority="
                + workflowPriority
                + ", executionNameSpace='"
                + executionNameSpace
                + '\''
                + ", isolationGroupId='"
                + isolationGroupId
                + '\''
                + ", iteration="
                + iteration
                + ", subWorkflowId='"
                + subWorkflowId
                + '\''
                + ", subworkflowChanged="
                + subworkflowChanged
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskModel taskModel = (TaskModel) o;
        return getRetryCount() == taskModel.getRetryCount()
                && getSeq() == taskModel.getSeq()
                && getPollCount() == taskModel.getPollCount()
                && getScheduledTime() == taskModel.getScheduledTime()
                && getStartTime() == taskModel.getStartTime()
                && getEndTime() == taskModel.getEndTime()
                && getUpdateTime() == taskModel.getUpdateTime()
                && getStartDelayInSeconds() == taskModel.getStartDelayInSeconds()
                && isRetried() == taskModel.isRetried()
                && isExecuted() == taskModel.isExecuted()
                && isCallbackFromWorker() == taskModel.isCallbackFromWorker()
                && getResponseTimeoutSeconds() == taskModel.getResponseTimeoutSeconds()
                && getCallbackAfterSeconds() == taskModel.getCallbackAfterSeconds()
                && getRateLimitPerFrequency() == taskModel.getRateLimitPerFrequency()
                && getRateLimitFrequencyInSeconds() == taskModel.getRateLimitFrequencyInSeconds()
                && getWorkflowPriority() == taskModel.getWorkflowPriority()
                && getIteration() == taskModel.getIteration()
                && isSubworkflowChanged() == taskModel.isSubworkflowChanged()
                && Objects.equals(getTaskType(), taskModel.getTaskType())
                && getStatus() == taskModel.getStatus()
                && Objects.equals(getInputData(), taskModel.getInputData())
                && Objects.equals(getReferenceTaskName(), taskModel.getReferenceTaskName())
                && Objects.equals(getCorrelationId(), taskModel.getCorrelationId())
                && Objects.equals(getTaskDefName(), taskModel.getTaskDefName())
                && Objects.equals(getRetriedTaskId(), taskModel.getRetriedTaskId())
                && Objects.equals(getWorkflowInstanceId(), taskModel.getWorkflowInstanceId())
                && Objects.equals(getWorkflowType(), taskModel.getWorkflowType())
                && Objects.equals(getTaskId(), taskModel.getTaskId())
                && Objects.equals(getReasonForIncompletion(), taskModel.getReasonForIncompletion())
                && Objects.equals(getWorkerId(), taskModel.getWorkerId())
                && Objects.equals(getWaitTimeout(), taskModel.getWaitTimeout())
                && Objects.equals(outputData, taskModel.outputData)
                && Objects.equals(outputPayload, taskModel.outputPayload)
                && Objects.equals(getWorkflowTask(), taskModel.getWorkflowTask())
                && Objects.equals(getDomain(), taskModel.getDomain())
                && Objects.equals(getInputMessage(), taskModel.getInputMessage())
                && Objects.equals(getOutputMessage(), taskModel.getOutputMessage())
                && Objects.equals(
                getExternalInputPayloadStoragePath(),
                taskModel.getExternalInputPayloadStoragePath())
                && Objects.equals(
                getExternalOutputPayloadStoragePath(),
                taskModel.getExternalOutputPayloadStoragePath())
                && Objects.equals(getExecutionNameSpace(), taskModel.getExecutionNameSpace())
                && Objects.equals(getIsolationGroupId(), taskModel.getIsolationGroupId())
                && Objects.equals(getSubWorkflowId(), taskModel.getSubWorkflowId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                getTaskType(),
                getStatus(),
                getInputData(),
                getReferenceTaskName(),
                getRetryCount(),
                getSeq(),
                getCorrelationId(),
                getPollCount(),
                getTaskDefName(),
                getScheduledTime(),
                getStartTime(),
                getEndTime(),
                getUpdateTime(),
                getStartDelayInSeconds(),
                getRetriedTaskId(),
                isRetried(),
                isExecuted(),
                isCallbackFromWorker(),
                getResponseTimeoutSeconds(),
                getWorkflowInstanceId(),
                getWorkflowType(),
                getTaskId(),
                getReasonForIncompletion(),
                getCallbackAfterSeconds(),
                getWorkerId(),
                getWaitTimeout(),
                outputData,
                outputPayload,
                getWorkflowTask(),
                getDomain(),
                getInputMessage(),
                getOutputMessage(),
                getRateLimitPerFrequency(),
                getRateLimitFrequencyInSeconds(),
                getExternalInputPayloadStoragePath(),
                getExternalOutputPayloadStoragePath(),
                getWorkflowPriority(),
                getExecutionNameSpace(),
                getIsolationGroupId(),
                getIteration(),
                getSubWorkflowId(),
                isSubworkflowChanged());
    }

    /**
     * 转换为对外 API 的 Task 对象。
     * 若输入/输出被外部化，对外返回空 map。
     */
    public Task toTask() {
        Task task = new Task();
        BeanUtils.copyProperties(this, task);
        task.setStatus(Task.Status.valueOf(status.name()));

        // 若输入/输出被外部化，对外返回空 map
        if (externalInputPayloadStoragePath != null) {
            task.setInputData(new HashMap<>());
        }
        if (externalOutputPayloadStoragePath != null) {
            task.setOutputData(new HashMap<>());
        }
        return task;
    }

    /** 把 TaskModel.Status 映射为对外 API 的 Task.Status */
    public static Task.Status mapToTaskStatus(TaskModel.Status status) {
        return Task.Status.valueOf(status.name());
    }

    /** 按 key 添加输入 */
    public void addInput(String key, Object value) {
        this.inputData.put(key, value);
    }

    /** 批量添加输入 */
    public void addInput(Map<String, Object> inputData) {
        if (inputData != null) {
            this.inputData.putAll(inputData);
        }
    }

    /** 按 key 添加输出 */
    public void addOutput(String key, Object value) {
        this.outputData.put(key, value);
    }

    /** 批量添加输出 */
    public void addOutput(Map<String, Object> outputData) {
        if (outputData != null) {
            this.outputData.putAll(outputData);
        }
    }

    /** 清空输出（含外部化数据与路径） */
    public void clearOutput() {
        this.outputData.clear();
        this.outputPayload.clear();
        this.externalOutputPayloadStoragePath = null;
    }
}