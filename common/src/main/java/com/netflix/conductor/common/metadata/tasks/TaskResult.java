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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.validation.constraints.NotEmpty;

import org.apache.commons.lang3.StringUtils;

import com.netflix.conductor.annotations.protogen.ProtoEnum;
import com.netflix.conductor.annotations.protogen.ProtoField;
import com.netflix.conductor.annotations.protogen.ProtoMessage;

import com.google.protobuf.Any;
import io.swagger.v3.oas.annotations.Hidden;

/**
 * 任务执行结果（TaskResult）模型类。
 *
 * <p>表示 worker 执行完任务后向 Conductor 汇报的结果，用于驱动任务状态的流转。
 * 它是 worker 与 Conductor 服务端交互的核心数据对象，通常由 worker 通过
 * {@code /tasks/{taskId}/ack} 或 {@code /tasks/{taskId}/update} 等接口提交。
 *
 * <p>通过 {@link ProtoMessage} 和 {@link ProtoField} 注解支持 Protobuf 序列化。
 */
@ProtoMessage
public class TaskResult {

    /**
     * 任务结果状态枚举。
     *
     * <p>相比 {@link Task.Status}，这里的取值更精简，只保留 worker 需要汇报的几种状态。
     */
    @ProtoEnum
    public enum Status {
        /** 任务仍在进行中（适用于长时间运行的任务） */
        IN_PROGRESS,
        /** 任务失败，可重试 */
        FAILED,
        /** 任务因终端错误失败，不再重试 */
        FAILED_WITH_TERMINAL_ERROR,
        /** 任务成功完成 */
        COMPLETED
    }

    /** 工作流实例 ID，不能为空 */
    @NotEmpty(message = "Workflow Id cannot be null or empty")
    @ProtoField(id = 1)
    private String workflowInstanceId;

    /** 任务 ID，不能为空 */
    @NotEmpty(message = "Task ID cannot be null or empty")
    @ProtoField(id = 2)
    private String taskId;

    /** 任务未完成的原因 */
    @ProtoField(id = 3)
    private String reasonForIncompletion;

    /** 回调延迟时间（秒），用于延迟队列实现 */
    @ProtoField(id = 4)
    private long callbackAfterSeconds;

    /** 执行该任务的 worker ID */
    @ProtoField(id = 5)
    private String workerId;

    /** 任务结果状态 */
    @ProtoField(id = 6)
    private Status status;

    /** 任务输出数据 */
    @ProtoField(id = 7)
    private Map<String, Object> outputData = new HashMap<>();

    /** 输出消息（Protobuf Any 类型），不对外暴露 */
    @ProtoField(id = 8)
    @Hidden
    private Any outputMessage;

    /** 任务执行日志列表，使用 CopyOnWriteArrayList 保证并发安全 */
    private List<TaskExecLog> logs = new CopyOnWriteArrayList<>();

    /** 任务输出负载的外部存储路径 */
    private String externalOutputPayloadStoragePath;

    /** 子工作流 ID */
    private String subWorkflowId;

    /** 是否延长任务租约 */
    private boolean extendLease;

    /**
     * 从 {@link Task} 构造 {@link TaskResult}，将任务状态映射为结果状态。
     *
     * <p>状态映射规则：
     * <ul>
     *   <li>CANCELED、COMPLETED_WITH_ERRORS、TIMED_OUT、SKIPPED → {@link Status#FAILED}</li>
     *   <li>SCHEDULED → {@link Status#IN_PROGRESS}</li>
     *   <li>其他状态按名称直接映射</li>
     * </ul>
     *
     * @param task 任务实例
     */
    public TaskResult(Task task) {
        this.workflowInstanceId = task.getWorkflowInstanceId();
        this.taskId = task.getTaskId();
        this.reasonForIncompletion = task.getReasonForIncompletion();
        this.callbackAfterSeconds = task.getCallbackAfterSeconds();
        this.workerId = task.getWorkerId();
        this.outputData = task.getOutputData();
        this.externalOutputPayloadStoragePath = task.getExternalOutputPayloadStoragePath();
        this.subWorkflowId = task.getSubWorkflowId();
        switch (task.getStatus()) {
            case CANCELED:
            case COMPLETED_WITH_ERRORS:
            case TIMED_OUT:
            case SKIPPED:
                this.status = Status.FAILED;
                break;
            case SCHEDULED:
                this.status = Status.IN_PROGRESS;
                break;
            default:
                this.status = Status.valueOf(task.getStatus().name());
                break;
        }
    }

    public TaskResult() {}

    /**
     * @return 产生该任务结果的工作流实例 ID
     */
    public String getWorkflowInstanceId() {
        return workflowInstanceId;
    }

    public void setWorkflowInstanceId(String workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
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

    /**
     * 设置任务未完成原因，最长保留 500 个字符。
     *
     * @param reasonForIncompletion 未完成原因
     */
    public void setReasonForIncompletion(String reasonForIncompletion) {
        this.reasonForIncompletion = StringUtils.substring(reasonForIncompletion, 0, 500);
    }

    public long getCallbackAfterSeconds() {
        return callbackAfterSeconds;
    }

    /**
     * 设置回调延迟时间。
     *
     * <p>当设置为非零值时，任务会在队列中保留指定秒数后才被 worker 轮询到，
     * 适用于长时间运行的任务：任务被更新为 IN_PROGRESS 后，在指定时间内不应被
     * 重新从队列中取出（延迟队列实现）。
     *
     * @param callbackAfterSeconds 任务在被交给轮询 worker 之前应在队列中保留的秒数
     */
    public void setCallbackAfterSeconds(long callbackAfterSeconds) {
        this.callbackAfterSeconds = callbackAfterSeconds;
    }

    public String getWorkerId() {
        return workerId;
    }

    /**
     * @param workerId 用于标识 worker 主机的自由格式字符串。可以是主机名、IP 地址或
     *     任何有意义的标识符，用于在排查问题时识别执行该任务的主机/进程。
     */
    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    /**
     * @return 任务结果状态
     */
    public Status getStatus() {
        return status;
    }

    /**
     * 设置任务结果状态。
     *
     * <p><b>IN_PROGRESS</b>：用于长时间运行的任务，表示任务仍在进行中，稍后需要再次检查。
     * 例如 worker 检查 DB 中的作业状态，而作业正由另一个进程执行。
     *
     * <p><b>FAILED、FAILED_WITH_TERMINAL_ERROR、COMPLETED</b>：任务的终态。
     * 当不希望任务被重试时，使用 FAILED_WITH_TERMINAL_ERROR。
     *
     * @param status 任务状态
     * @see #setCallbackAfterSeconds(long)
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    public Map<String, Object> getOutputData() {
        return outputData;
    }

    /**
     * @param outputData 任务执行结果的输出数据
     */
    public void setOutputData(Map<String, Object> outputData) {
        this.outputData = outputData;
    }

    /**
     * 添加一条输出数据。
     *
     * @param key 输出字段名
     * @param value 输出值
     * @return 当前 TaskResult 实例，便于链式调用
     */
    public TaskResult addOutputData(String key, Object value) {
        this.outputData.put(key, value);
        return this;
    }

    public Any getOutputMessage() {
        return outputMessage;
    }

    public void setOutputMessage(Any outputMessage) {
        this.outputMessage = outputMessage;
    }

    /**
     * @return 任务执行日志列表
     */
    public List<TaskExecLog> getLogs() {
        return logs;
    }

    /**
     * @param logs 任务执行日志列表
     */
    public void setLogs(List<TaskExecLog> logs) {
        this.logs = logs;
    }

    /**
     * 追加一条任务执行日志。
     *
     * @param log 要添加的日志行
     * @return 当前 TaskResult 实例，便于链式调用
     */
    public TaskResult log(String log) {
        this.logs.add(new TaskExecLog(log));
        return this;
    }

    /**
     * @return 任务输出在外部存储中的路径
     */
    public String getExternalOutputPayloadStoragePath() {
        return externalOutputPayloadStoragePath;
    }

    /**
     * @param externalOutputPayloadStoragePath 任务输出在外部存储中的路径
     */
    public void setExternalOutputPayloadStoragePath(String externalOutputPayloadStoragePath) {
        this.externalOutputPayloadStoragePath = externalOutputPayloadStoragePath;
    }

    public String getSubWorkflowId() {
        return subWorkflowId;
    }

    public void setSubWorkflowId(String subWorkflowId) {
        this.subWorkflowId = subWorkflowId;
    }

    public boolean isExtendLease() {
        return extendLease;
    }

    public void setExtendLease(boolean extendLease) {
        this.extendLease = extendLease;
    }

    @Override
    public String toString() {
        return "TaskResult{"
                + "workflowInstanceId='"
                + workflowInstanceId
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
                + ", status="
                + status
                + ", outputData="
                + outputData
                + ", outputMessage="
                + outputMessage
                + ", logs="
                + logs
                + ", externalOutputPayloadStoragePath='"
                + externalOutputPayloadStoragePath
                + '\''
                + ", subWorkflowId='"
                + subWorkflowId
                + '\''
                + ", extendLease='"
                + extendLease
                + '\''
                + '}';
    }

    /** 创建一个表示"任务成功完成"的结果对象 */
    public static TaskResult complete() {
        return newTaskResult(Status.COMPLETED);
    }

    /** 创建一个表示"任务失败"的结果对象 */
    public static TaskResult failed() {
        return newTaskResult(Status.FAILED);
    }

    /**
     * 创建一个表示"任务失败"的结果对象，并附带失败原因。
     *
     * @param failureReason 失败原因
     * @return 任务结果对象
     */
    public static TaskResult failed(String failureReason) {
        TaskResult result = newTaskResult(Status.FAILED);
        result.setReasonForIncompletion(failureReason);
        return result;
    }

    /** 创建一个表示"任务进行中"的结果对象 */
    public static TaskResult inProgress() {
        return newTaskResult(Status.IN_PROGRESS);
    }

    /**
     * 根据给定状态创建任务结果对象。
     *
     * @param status 任务结果状态
     * @return 任务结果对象
     */
    public static TaskResult newTaskResult(Status status) {
        TaskResult result = new TaskResult();
        result.setStatus(status);
        return result;
    }
}