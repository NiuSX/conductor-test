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
package com.netflix.conductor.common.metadata.tasks;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import com.netflix.conductor.annotations.protogen.ProtoEnum;
import com.netflix.conductor.annotations.protogen.ProtoField;
import com.netflix.conductor.annotations.protogen.ProtoMessage;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;

import com.google.protobuf.Any;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * 任务（Task）模型类。
 *
 * <p>表示 Conductor 工作流中一个具体的任务实例，包含任务类型、状态、输入输出数据、时间信息、
 * 重试信息、工作流关联信息等。它是工作流执行过程中任务调度的核心数据结构。
 *
 * <p>该类通过 {@link ProtoMessage} 和 {@link ProtoField} 注解支持 Protobuf 序列化，
 * 字段的 id 对应 Protobuf 中的字段编号。
 */
@ProtoMessage
public class Task {

    /**
     * 任务状态枚举。
     *
     * <p>每个状态包含三个布尔属性：
     * <ul>
     *   <li>{@code terminal}：是否为终态，终态任务不会再发生变化</li>
     *   <li>{@code successful}：是否表示成功完成</li>
     *   <li>{@code retriable}：是否允许重试</li>
     * </ul>
     */
    @ProtoEnum
    public enum Status {
        /** 任务正在执行中 */
        IN_PROGRESS(false, true, true),
        /** 任务被取消 */
        CANCELED(true, false, false),
        /** 任务执行失败，可重试 */
        FAILED(true, false, true),
        /**
         * 任务因终端错误失败。
         * 即使配置了重试也不会重试，任务及关联工作流应被终止。
         */
        FAILED_WITH_TERMINAL_ERROR(
                true, false,
                false), // 即使配置了重试也不会重试，任务和关联的工作流应被终止
        /** 任务成功完成 */
        COMPLETED(true, true, true),
        /** 任务完成但带有错误 */
        COMPLETED_WITH_ERRORS(true, true, true),
        /** 任务已调度但尚未开始 */
        SCHEDULED(false, true, true),
        /** 任务超时 */
        TIMED_OUT(true, false, true),
        /** 任务被跳过 */
        SKIPPED(true, true, false);

        /** 是否为终态 */
        private final boolean terminal;

        /** 是否成功 */
        private final boolean successful;

        /** 是否可重试 */
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

    /** 任务类型，例如 HTTP、SIMPLE、SUB_WORKFLOW 等 */
    @ProtoField(id = 1)
    private String taskType;

    /** 任务当前状态 */
    @ProtoField(id = 2)
    private Status status;

    /** 任务输入数据 */
    @ProtoField(id = 3)
    private Map<String, Object> inputData = new HashMap<>();

    /** 任务引用名称，通常用于在工作流定义中标识任务 */
    @ProtoField(id = 4)
    private String referenceTaskName;

    /** 重试次数 */
    @ProtoField(id = 5)
    private int retryCount;

    /** 任务在工作流中的序号 */
    @ProtoField(id = 6)
    private int seq;

    /** 关联 ID，用于关联外部系统或业务标识 */
    @ProtoField(id = 7)
    private String correlationId;

    /** 任务被轮询的次数 */
    @ProtoField(id = 8)
    private int pollCount;

    /** 任务定义名称 */
    @ProtoField(id = 9)
    private String taskDefName;

    /** 任务被调度的时间 */
    @ProtoField(id = 10)
    private long scheduledTime;

    /** 任务第一次被轮询的时间 */
    @ProtoField(id = 11)
    private long startTime;

    /** 任务执行完成的时间 */
    @ProtoField(id = 12)
    private long endTime;

    /** 任务最后一次更新的时间 */
    @ProtoField(id = 13)
    private long updateTime;

    /** 任务开始前的延迟时间（秒） */
    @ProtoField(id = 14)
    private int startDelayInSeconds;

    /** 重试任务 ID */
    @ProtoField(id = 15)
    private String retriedTaskId;

    /** 该任务是否已经被重试过 */
    @ProtoField(id = 16)
    private boolean retried;

    /** 该任务是否已经在 Conductor 中完成整个生命周期（从开始到完成并更新到数据存储） */
    @ProtoField(id = 17)
    private boolean executed;

    /** 是否由 worker 回调，默认为 true */
    @ProtoField(id = 18)
    private boolean callbackFromWorker = true;

    /** 响应超时时间（秒），超过该时间任务会被重新入队 */
    @ProtoField(id = 19)
    private long responseTimeoutSeconds;

    /** 工作流实例 ID */
    @ProtoField(id = 20)
    private String workflowInstanceId;

    /** 工作流类型/名称 */
    @ProtoField(id = 21)
    private String workflowType;

    /** 任务 ID */
    @ProtoField(id = 22)
    private String taskId;

    /** 任务未完成的原因 */
    @ProtoField(id = 23)
    private String reasonForIncompletion;

    /** 回调延迟时间（秒） */
    @ProtoField(id = 24)
    private long callbackAfterSeconds;

    /** 执行该任务的 worker ID */
    @ProtoField(id = 25)
    private String workerId;

    /** 任务输出数据 */
    @ProtoField(id = 26)
    private Map<String, Object> outputData = new HashMap<>();

    /** 工作流任务定义 */
    @ProtoField(id = 27)
    private WorkflowTask workflowTask;

    /** 域（Domain）信息，用于多租户或隔离 */
    @ProtoField(id = 28)
    private String domain;

    /** 输入消息（Protobuf Any 类型），不对外暴露 */
    @ProtoField(id = 29)
    @Hidden
    private Any inputMessage;

    /** 输出消息（Protobuf Any 类型），不对外暴露 */
    @ProtoField(id = 30)
    @Hidden
    private Any outputMessage;

    // id 31 保留

    /** 每个频率允许的速率限制 */
    @ProtoField(id = 32)
    private int rateLimitPerFrequency;

    /** 速率限制频率（秒） */
    @ProtoField(id = 33)
    private int rateLimitFrequencyInSeconds;

    /** 外部输入负载存储路径 */
    @ProtoField(id = 34)
    private String externalInputPayloadStoragePath;

    /** 外部输出负载存储路径 */
    @ProtoField(id = 35)
    private String externalOutputPayloadStoragePath;

    /** 工作流优先级 */
    @ProtoField(id = 36)
    private int workflowPriority;

    /** 执行命名空间 */
    @ProtoField(id = 37)
    private String executionNameSpace;

    /** 隔离组 ID */
    @ProtoField(id = 38)
    private String isolationGroupId;

    /** 迭代次数，用于循环任务 */
    @ProtoField(id = 40)
    private int iteration;

    /** 子工作流 ID */
    @ProtoField(id = 41)
    private String subWorkflowId;

    /**
     * 用于标记与 SUB_WORKFLOW 任务关联的子工作流是否被直接操作过。
     */
    @ProtoField(id = 42)
    private boolean subworkflowChanged;

    public Task() {}

    /**
     * @return 任务类型
     * @see TaskType
     */
    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    /**
     * @return 任务状态
     */
    public Status getStatus() {
        return status;
    }

    /**
     * @param status 任务状态
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    public Map<String, Object> getInputData() {
        return inputData;
    }

    public void setInputData(Map<String, Object> inputData) {
        if (inputData == null) {
            inputData = new HashMap<>();
        }
        this.inputData = inputData;
    }

    /**
     * @return 引用任务名称
     */
    public String getReferenceTaskName() {
        return referenceTaskName;
    }

    /**
     * @param referenceTaskName 引用任务名称
     */
    public void setReferenceTaskName(String referenceTaskName) {
        this.referenceTaskName = referenceTaskName;
    }

    /**
     * @return 关联 ID
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * @param correlationId 关联 ID
     */
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * @return 重试次数
     */
    public int getRetryCount() {
        return retryCount;
    }

    /**
     * @param retryCount 重试次数
     */
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    /**
     * @return 调度时间
     */
    public long getScheduledTime() {
        return scheduledTime;
    }

    /**
     * @param scheduledTime 调度时间
     */
    public void setScheduledTime(long scheduledTime) {
        this.scheduledTime = scheduledTime;
    }

    /**
     * @return 开始时间
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * @param startTime 开始时间
     */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * @return 结束时间
     */
    public long getEndTime() {
        return endTime;
    }

    /**
     * @param endTime 结束时间
     */
    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    /**
     * @return 开始延迟时间（秒）
     */
    public int getStartDelayInSeconds() {
        return startDelayInSeconds;
    }

    /**
     * @param startDelayInSeconds 开始延迟时间（秒）
     */
    public void setStartDelayInSeconds(int startDelayInSeconds) {
        this.startDelayInSeconds = startDelayInSeconds;
    }

    /**
     * @return 重试任务 ID
     */
    public String getRetriedTaskId() {
        return retriedTaskId;
    }

    /**
     * @param retriedTaskId 重试任务 ID
     */
    public void setRetriedTaskId(String retriedTaskId) {
        this.retriedTaskId = retriedTaskId;
    }

    /**
     * @return 任务序号
     */
    public int getSeq() {
        return seq;
    }

    /**
     * @param seq 任务序号
     */
    public void setSeq(int seq) {
        this.seq = seq;
    }

    /**
     * @return 最后更新时间
     */
    public long getUpdateTime() {
        return updateTime;
    }

    /**
     * @param updateTime 最后更新时间
     */
    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    /**
     * 计算队列等待时间。
     *
     * <p>如果任务已经回调并且设置了 callbackAfterSeconds，则返回从预期回调时间到现在的等待时间；
     * 否则返回 startTime - scheduledTime。
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
     * @return 任务失败后是否已被重试
     */
    public boolean isRetried() {
        return retried;
    }

    /**
     * @param retried 是否已被重试
     */
    public void setRetried(boolean retried) {
        this.retried = retried;
    }

    /**
     * @return 任务是否已在 Conductor 中完成整个生命周期（从开始到完成并更新到数据存储）
     */
    public boolean isExecuted() {
        return executed;
    }

    /**
     * @param executed 是否已执行完成
     */
    public void setExecuted(boolean executed) {
        this.executed = executed;
    }

    /**
     * @return 任务被轮询的次数
     */
    public int getPollCount() {
        return pollCount;
    }

    public void setPollCount(int pollCount) {
        this.pollCount = pollCount;
    }

    /** 增加轮询计数 */
    public void incrementPollCount() {
        ++this.pollCount;
    }

    public boolean isCallbackFromWorker() {
        return callbackFromWorker;
    }

    public void setCallbackFromWorker(boolean callbackFromWorker) {
        this.callbackFromWorker = callbackFromWorker;
    }

    /**
     * 获取任务定义名称。如果未设置，则回退到任务类型。
     *
     * @return 任务定义名称
     */
    public String getTaskDefName() {
        if (taskDefName == null || "".equals(taskDefName)) {
            taskDefName = taskType;
        }
        return taskDefName;
    }

    /**
     * @param taskDefName 任务定义名称
     */
    public void setTaskDefName(String taskDefName) {
        this.taskDefName = taskDefName;
    }

    /**
     * @return 任务响应超时时间（秒），超过该时间任务会被重新入队
     */
    public long getResponseTimeoutSeconds() {
        return responseTimeoutSeconds;
    }

    /**
     * @param responseTimeoutSeconds 任务响应超时时间（秒）
     */
    public void setResponseTimeoutSeconds(long responseTimeoutSeconds) {
        this.responseTimeoutSeconds = responseTimeoutSeconds;
    }

    /**
     * @return 工作流实例 ID
     */
    public String getWorkflowInstanceId() {
        return workflowInstanceId;
    }

    /**
     * @param workflowInstanceId 工作流实例 ID
     */
    public void setWorkflowInstanceId(String workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
    }

    public String getWorkflowType() {
        return workflowType;
    }

    /**
     * 设置工作流类型，并返回当前任务对象以便链式调用。
     *
     * @param workflowType 工作流名称
     * @return 当前任务对象
     */
    public com.netflix.conductor.common.metadata.tasks.Task setWorkflowType(String workflowType) {
        this.workflowType = workflowType;
        return this;
    }

    /**
     * @return 任务 ID
     */
    public String getTaskId() {
        return taskId;
    }

    /**
     * @param taskId 任务 ID
     */
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    /**
     * @return 任务未完成的原因
     */
    public String getReasonForIncompletion() {
        return reasonForIncompletion;
    }

    /**
     * 设置任务未完成原因，最长保留 500 个字符。
     *
     * @param reasonForIncompletion 未完成原因
     */
    public void setReasonForIncompletion(String reasonForIncompletion) {
        this.reasonForIncompletion = StringUtils.substring(reasonForIncompletion, 0, 500);
    }

    /**
     * @return 回调延迟时间（秒）
     */
    public long getCallbackAfterSeconds() {
        return callbackAfterSeconds;
    }

    /**
     * @param callbackAfterSeconds 回调延迟时间（秒）
     */
    public void setCallbackAfterSeconds(long callbackAfterSeconds) {
        this.callbackAfterSeconds = callbackAfterSeconds;
    }

    /**
     * @return worker ID
     */
    public String getWorkerId() {
        return workerId;
    }

    /**
     * @param workerId worker ID
     */
    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    /**
     * @return 输出数据
     */
    public Map<String, Object> getOutputData() {
        return outputData;
    }

    /**
     * @param outputData 输出数据
     */
    public void setOutputData(Map<String, Object> outputData) {
        if (outputData == null) {
            outputData = new HashMap<>();
        }
        this.outputData = outputData;
    }

    /**
     * @return 工作流任务定义
     */
    public WorkflowTask getWorkflowTask() {
        return workflowTask;
    }

    /**
     * @param workflowTask 工作流任务定义
     */
    public void setWorkflowTask(WorkflowTask workflowTask) {
        this.workflowTask = workflowTask;
    }

    /**
     * @return 域信息
     */
    public String getDomain() {
        return domain;
    }

    /**
     * @param domain 域信息
     */
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

    /**
     * 获取任务定义（如果可用）。
     *
     * @return 包含任务定义的 {@link Optional}
     */
    public Optional<TaskDef> getTaskDefinition() {
        return Optional.ofNullable(this.getWorkflowTask()).map(WorkflowTask::getTaskDefinition);
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

    /**
     * @return 任务输入负载的外部存储路径
     */
    public String getExternalInputPayloadStoragePath() {
        return externalInputPayloadStoragePath;
    }

    /**
     * @param externalInputPayloadStoragePath 任务输入负载的外部存储路径
     */
    public void setExternalInputPayloadStoragePath(String externalInputPayloadStoragePath) {
        this.externalInputPayloadStoragePath = externalInputPayloadStoragePath;
    }

    /**
     * @return 任务输出负载的外部存储路径
     */
    public String getExternalOutputPayloadStoragePath() {
        return externalOutputPayloadStoragePath;
    }

    /**
     * @param externalOutputPayloadStoragePath 任务输出负载的外部存储路径
     */
    public void setExternalOutputPayloadStoragePath(String externalOutputPayloadStoragePath) {
        this.externalOutputPayloadStoragePath = externalOutputPayloadStoragePath;
    }

    public void setIsolationGroupId(String isolationGroupId) {
        this.isolationGroupId = isolationGroupId;
    }

    public String getIsolationGroupId() {
        return isolationGroupId;
    }

    public String getExecutionNameSpace() {
        return executionNameSpace;
    }

    public void setExecutionNameSpace(String executionNameSpace) {
        this.executionNameSpace = executionNameSpace;
    }

    /**
     * @return 迭代次数
     */
    public int getIteration() {
        return iteration;
    }

    /**
     * @param iteration 迭代次数
     */
    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    /**
     * 是否为循环任务（迭代次数大于 0）。
     *
     * @return 如果是循环任务返回 true
     */
    public boolean isLoopOverTask() {
        return iteration > 0;
    }

    /** @return 工作流上定义的优先级 */
    public int getWorkflowPriority() {
        return workflowPriority;
    }

    /**
     * @param workflowPriority 工作流优先级
     */
    public void setWorkflowPriority(int workflowPriority) {
        this.workflowPriority = workflowPriority;
    }

    public boolean isSubworkflowChanged() {
        return subworkflowChanged;
    }

    public void setSubworkflowChanged(boolean subworkflowChanged) {
        this.subworkflowChanged = subworkflowChanged;
    }

    /**
     * 获取子工作流 ID。
     *
     * <p>为了向后兼容，如果 {@code subWorkflowId} 为空，则会尝试从输出数据或输入数据中获取
     * {@code subWorkflowId}。
     *
     * @return 子工作流 ID，可能为 null
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

    /**
     * 设置子工作流 ID。
     *
     * <p>为了向后兼容，如果输出数据中已存在 {@code subWorkflowId}，也会同步更新。
     *
     * @param subWorkflowId 子工作流 ID
     */
    public void setSubWorkflowId(String subWorkflowId) {
        this.subWorkflowId = subWorkflowId;
        // 向后兼容
        if (this.getOutputData() != null && this.getOutputData().containsKey("subWorkflowId")) {
            this.getOutputData().put("subWorkflowId", subWorkflowId);
        }
    }

    /**
     * 创建当前任务的一个浅拷贝。
     *
     * <p>注意：拷贝过程中不会复制 {@code retried}、{@code updateTime}、{@code retriedTaskId} 等字段。
     *
     * @return 任务副本
     */
    public Task copy() {
        Task copy = new Task();
        copy.setCallbackAfterSeconds(callbackAfterSeconds);
        copy.setCallbackFromWorker(callbackFromWorker);
        copy.setCorrelationId(correlationId);
        copy.setInputData(inputData);
        copy.setOutputData(outputData);
        copy.setReferenceTaskName(referenceTaskName);
        copy.setStartDelayInSeconds(startDelayInSeconds);
        copy.setTaskDefName(taskDefName);
        copy.setTaskType(taskType);
        copy.setWorkflowInstanceId(workflowInstanceId);
        copy.setWorkflowType(workflowType);
        copy.setResponseTimeoutSeconds(responseTimeoutSeconds);
        copy.setStatus(status);
        copy.setRetryCount(retryCount);
        copy.setPollCount(pollCount);
        copy.setTaskId(taskId);
        copy.setWorkflowTask(workflowTask);
        copy.setDomain(domain);
        copy.setInputMessage(inputMessage);
        copy.setOutputMessage(outputMessage);
        copy.setRateLimitPerFrequency(rateLimitPerFrequency);
        copy.setRateLimitFrequencyInSeconds(rateLimitFrequencyInSeconds);
        copy.setExternalInputPayloadStoragePath(externalInputPayloadStoragePath);
        copy.setExternalOutputPayloadStoragePath(externalOutputPayloadStoragePath);
        copy.setWorkflowPriority(workflowPriority);
        copy.setIteration(iteration);
        copy.setExecutionNameSpace(executionNameSpace);
        copy.setIsolationGroupId(isolationGroupId);
        copy.setSubWorkflowId(getSubWorkflowId());
        copy.setSubworkflowChanged(subworkflowChanged);

        return copy;
    }

    /**
     * 创建当前任务的深拷贝。
     *
     * <p>在 {@link #copy()} 的基础上，额外复制了时间、worker、完成原因、序号等字段。
     * 用于 copy Workflow 方法中提供有效的深拷贝对象。
     *
     * <p>注意：以下字段不会被复制：
     * <ul>
     *   <li>retried</li>
     *   <li>updateTime</li>
     *   <li>retriedTaskId</li>
     * </ul>
     *
     * @return 任务深拷贝
     */
    public Task deepCopy() {
        Task deepCopy = copy();
        deepCopy.setStartTime(startTime);
        deepCopy.setScheduledTime(scheduledTime);
        deepCopy.setEndTime(endTime);
        deepCopy.setWorkerId(workerId);
        deepCopy.setReasonForIncompletion(reasonForIncompletion);
        deepCopy.setSeq(seq);

        return deepCopy;
    }

    @Override
    public String toString() {
        return "Task{"
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
                + ", inputMessage='"
                + inputMessage
                + '\''
                + ", outputMessage='"
                + outputMessage
                + '\''
                + ", rateLimitPerFrequency="
                + rateLimitPerFrequency
                + ", rateLimitFrequencyInSeconds="
                + rateLimitFrequencyInSeconds
                + ", workflowPriority="
                + workflowPriority
                + ", externalInputPayloadStoragePath='"
                + externalInputPayloadStoragePath
                + '\''
                + ", externalOutputPayloadStoragePath='"
                + externalOutputPayloadStoragePath
                + '\''
                + ", isolationGroupId='"
                + isolationGroupId
                + '\''
                + ", executionNameSpace='"
                + executionNameSpace
                + '\''
                + ", subworkflowChanged='"
                + subworkflowChanged
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Task task = (Task) o;
        return getRetryCount() == task.getRetryCount()
                && getSeq() == task.getSeq()
                && getPollCount() == task.getPollCount()
                && getScheduledTime() == task.getScheduledTime()
                && getStartTime() == task.getStartTime()
                && getEndTime() == task.getEndTime()
                && getUpdateTime() == task.getUpdateTime()
                && getStartDelayInSeconds() == task.getStartDelayInSeconds()
                && isRetried() == task.isRetried()
                && isExecuted() == task.isExecuted()
                && isCallbackFromWorker() == task.isCallbackFromWorker()
                && getResponseTimeoutSeconds() == task.getResponseTimeoutSeconds()
                && getCallbackAfterSeconds() == task.getCallbackAfterSeconds()
                && getRateLimitPerFrequency() == task.getRateLimitPerFrequency()
                && getRateLimitFrequencyInSeconds() == task.getRateLimitFrequencyInSeconds()
                && Objects.equals(getTaskType(), task.getTaskType())
                && getStatus() == task.getStatus()
                && getIteration() == task.getIteration()
                && getWorkflowPriority() == task.getWorkflowPriority()
                && Objects.equals(getInputData(), task.getInputData())
                && Objects.equals(getReferenceTaskName(), task.getReferenceTaskName())
                && Objects.equals(getCorrelationId(), task.getCorrelationId())
                && Objects.equals(getTaskDefName(), task.getTaskDefName())
                && Objects.equals(getRetriedTaskId(), task.getRetriedTaskId())
                && Objects.equals(getWorkflowInstanceId(), task.getWorkflowInstanceId())
                && Objects.equals(getWorkflowType(), task.getWorkflowType())
                && Objects.equals(getTaskId(), task.getTaskId())
                && Objects.equals(getReasonForIncompletion(), task.getReasonForIncompletion())
                && Objects.equals(getWorkerId(), task.getWorkerId())
                && Objects.equals(getOutputData(), task.getOutputData())
                && Objects.equals(getWorkflowTask(), task.getWorkflowTask())
                && Objects.equals(getDomain(), task.getDomain())
                && Objects.equals(getInputMessage(), task.getInputMessage())
                && Objects.equals(getOutputMessage(), task.getOutputMessage())
                && Objects.equals(
                getExternalInputPayloadStoragePath(),
                task.getExternalInputPayloadStoragePath())
                && Objects.equals(
                getExternalOutputPayloadStoragePath(),
                task.getExternalOutputPayloadStoragePath())
                && Objects.equals(getIsolationGroupId(), task.getIsolationGroupId())
                && Objects.equals(getExecutionNameSpace(), task.getExecutionNameSpace());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                getTaskType(),
                getStatus(),
                getInputData(),
                getReferenceTaskName(),
                getWorkflowPriority(),
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
                getOutputData(),
                getWorkflowTask(),
                getDomain(),
                getInputMessage(),
                getOutputMessage(),
                getRateLimitPerFrequency(),
                getRateLimitFrequencyInSeconds(),
                getExternalInputPayloadStoragePath(),
                getExternalOutputPayloadStoragePath(),
                getIsolationGroupId(),
                getExecutionNameSpace());
    }
}