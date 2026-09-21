/*
 * Copyright 2021 Netflix, Inc.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.validation.Valid;
import javax.validation.constraints.Email;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import com.netflix.conductor.annotations.protogen.ProtoEnum;
import com.netflix.conductor.annotations.protogen.ProtoField;
import com.netflix.conductor.annotations.protogen.ProtoMessage;
import com.netflix.conductor.common.constraints.OwnerEmailMandatoryConstraint;
import com.netflix.conductor.common.constraints.TaskTimeoutConstraint;
import com.netflix.conductor.common.metadata.BaseDef;

/**
 * 任务定义（TaskDef）元数据类。
 *
 * <p>描述“某一类任务”应该如何被调度和执行，是任务实例（{@link Task}）的模板。
 * 包含任务的超时策略、重试策略、限流、并发限制、输入输出键等配置。
 *
 * <p>通过 {@link ProtoMessage} 和 {@link ProtoField} 注解支持 Protobuf 序列化；
 * 同时使用 JSR-303 校验注解进行参数校验。
 *
 * <p>类级别注解：
 * <ul>
 *   <li>{@link TaskTimeoutConstraint}：自定义校验，确保 timeoutSeconds 与 responseTimeoutSeconds 的关系合法</li>
 *   <li>{@link Valid}：开启级联校验</li>
 * </ul>
 */
@ProtoMessage
@TaskTimeoutConstraint
@Valid
public class TaskDef extends BaseDef {

    /**
     * 任务超时策略枚举。
     */
    @ProtoEnum
    public enum TimeoutPolicy {
        /** 超时后重试任务 */
        RETRY,
        /** 超时后终止整个工作流 */
        TIME_OUT_WF,
        /** 仅告警，不做其他处理 */
        ALERT_ONLY
    }

    /**
     * 重试逻辑枚举，决定重试间隔如何计算。
     */
    @ProtoEnum
    public enum RetryLogic {
        /** 固定间隔重试 */
        FIXED,
        /** 指数退避重试 */
        EXPONENTIAL_BACKOFF,
        /** 线性退避重试 */
        LINEAR_BACKOFF
    }

    /** 一小时的秒数常量 */
    public static final int ONE_HOUR = 60 * 60;

    /** 任务唯一名称，全局唯一 */
    @NotEmpty(message = "TaskDef name cannot be null or empty")
    @ProtoField(id = 1)
    private String name;

    /** 任务描述 */
    @ProtoField(id = 2)
    private String description;

    /** 重试次数，默认 3 次，必须 >= 0 */
    @ProtoField(id = 3)
    @Min(value = 0, message = "TaskDef retryCount: {value} must be >= 0")
    private int retryCount = 3; // 默认值

    /** 任务超时时间（秒），必须非空 */
    @ProtoField(id = 4)
    @NotNull
    private long timeoutSeconds;

    /** 任务输入参数键列表 */
    @ProtoField(id = 5)
    private List<String> inputKeys = new ArrayList<>();

    /** 任务输出参数键列表 */
    @ProtoField(id = 6)
    private List<String> outputKeys = new ArrayList<>();

    /** 超时策略，默认超时后终止工作流 */
    @ProtoField(id = 7)
    private TimeoutPolicy timeoutPolicy = TimeoutPolicy.TIME_OUT_WF;

    /** 重试逻辑，默认固定间隔 */
    @ProtoField(id = 8)
    private RetryLogic retryLogic = RetryLogic.FIXED;

    /** 重试延迟时间（秒），默认 60 秒 */
    @ProtoField(id = 9)
    private int retryDelaySeconds = 60;

    /** 响应超时时间（秒），最小 1 秒，默认 1 小时 */
    @ProtoField(id = 10)
    @Min(
            value = 1,
            message =
                    "TaskDef responseTimeoutSeconds: ${validatedValue} should be minimum {value} second")
    private long responseTimeoutSeconds = ONE_HOUR;

    /** 并发执行限制，同一时间允许处于 IN_PROGRESS 状态的最大任务数 */
    @ProtoField(id = 11)
    private Integer concurrentExecLimit;

    /** 输入模板，用于在运行时生成任务输入 */
    @ProtoField(id = 12)
    private Map<String, Object> inputTemplate = new HashMap<>();

    // 该字段已废弃，请勿使用 id 13。
    //	@ProtoField(id = 13)
    //	private Integer rateLimitPerSecond;

    /** 每个限流频率周期内允许执行的最大任务数 */
    @ProtoField(id = 14)
    private Integer rateLimitPerFrequency;

    /** 限流频率周期（秒） */
    @ProtoField(id = 15)
    private Integer rateLimitFrequencyInSeconds;

    /** 隔离组 ID */
    @ProtoField(id = 16)
    private String isolationGroupId;

    /** 执行命名空间 */
    @ProtoField(id = 17)
    private String executionNameSpace;

    /** 任务负责人邮箱，必须为合法邮箱地址 */
    @ProtoField(id = 18)
    @OwnerEmailMandatoryConstraint
    @Email(message = "ownerEmail should be valid email address")
    private String ownerEmail;

    /** 轮询超时时间（秒），必须 >= 0 */
    @ProtoField(id = 19)
    @Min(value = 0, message = "TaskDef pollTimeoutSeconds: {value} must be >= 0")
    private Integer pollTimeoutSeconds;

    /** 退避比例因子，适用于 LINEAR_BACKOFF，最小为 1 */
    @ProtoField(id = 20)
    @Min(value = 1, message = "Backoff scale factor. Applicable for LINEAR_BACKOFF")
    private Integer backoffScaleFactor = 1;

    public TaskDef() {}

    public TaskDef(String name) {
        this.name = name;
    }

    public TaskDef(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public TaskDef(String name, String description, int retryCount, long timeoutSeconds) {
        this.name = name;
        this.description = description;
        this.retryCount = retryCount;
        this.timeoutSeconds = timeoutSeconds;
    }

    public TaskDef(
            String name,
            String description,
            String ownerEmail,
            int retryCount,
            long timeoutSeconds,
            long responseTimeoutSeconds) {
        this.name = name;
        this.description = description;
        this.ownerEmail = ownerEmail;
        this.retryCount = retryCount;
        this.timeoutSeconds = timeoutSeconds;
        this.responseTimeoutSeconds = responseTimeoutSeconds;
    }

    /**
     * @return 任务名称
     */
    public String getName() {
        return name;
    }

    /**
     * @param name 任务名称
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return 任务描述
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param description 任务描述
     */
    public void setDescription(String description) {
        this.description = description;
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
     * @return 超时时间（秒）
     */
    public long getTimeoutSeconds() {
        return timeoutSeconds;
    }

    /**
     * @param timeoutSeconds 超时时间（秒）
     */
    public void setTimeoutSeconds(long timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    /**
     * @return 输入键列表
     */
    public List<String> getInputKeys() {
        return inputKeys;
    }

    /**
     * @param inputKeys 任务输入 map 中允许接受的键集合
     */
    public void setInputKeys(List<String> inputKeys) {
        this.inputKeys = inputKeys;
    }

    /**
     * @return 任务执行完成后的输出键列表
     */
    public List<String> getOutputKeys() {
        return outputKeys;
    }

    /**
     * @param outputKeys 输出键列表
     */
    public void setOutputKeys(List<String> outputKeys) {
        this.outputKeys = outputKeys;
    }

    /**
     * @return 超时策略
     */
    public TimeoutPolicy getTimeoutPolicy() {
        return timeoutPolicy;
    }

    /**
     * @param timeoutPolicy 超时策略
     */
    public void setTimeoutPolicy(TimeoutPolicy timeoutPolicy) {
        this.timeoutPolicy = timeoutPolicy;
    }

    /**
     * @return 重试逻辑
     */
    public RetryLogic getRetryLogic() {
        return retryLogic;
    }

    /**
     * @param retryLogic 重试逻辑
     */
    public void setRetryLogic(RetryLogic retryLogic) {
        this.retryLogic = retryLogic;
    }

    /**
     * @return 重试延迟时间（秒）
     */
    public int getRetryDelaySeconds() {
        return retryDelaySeconds;
    }

    /**
     * @return 任务响应超时时间（秒），超过该时间任务会被重新入队
     */
    public long getResponseTimeoutSeconds() {
        return responseTimeoutSeconds;
    }

    /**
     * @param responseTimeoutSeconds 任务响应超时时间（秒），超过该时间任务会被重新入队
     */
    public void setResponseTimeoutSeconds(long responseTimeoutSeconds) {
        this.responseTimeoutSeconds = responseTimeoutSeconds;
    }

    /**
     * @param retryDelaySeconds 重试延迟时间（秒）
     */
    public void setRetryDelaySeconds(int retryDelaySeconds) {
        this.retryDelaySeconds = retryDelaySeconds;
    }

    /**
     * @return 输入模板
     */
    public Map<String, Object> getInputTemplate() {
        return inputTemplate;
    }

    /**
     * @return 每个限流周期内允许执行的最大任务数；未设置时返回 0
     */
    public Integer getRateLimitPerFrequency() {
        return rateLimitPerFrequency == null ? 0 : rateLimitPerFrequency;
    }

    /**
     * @param rateLimitPerFrequency 每个限流周期内允许执行的最大任务数；设置为 0 表示移除限流
     */
    public void setRateLimitPerFrequency(Integer rateLimitPerFrequency) {
        this.rateLimitPerFrequency = rateLimitPerFrequency;
    }

    /**
     * @return 限流时间窗口（秒）；未设置时默认返回 1 秒
     */
    public Integer getRateLimitFrequencyInSeconds() {
        return rateLimitFrequencyInSeconds == null ? 1 : rateLimitFrequencyInSeconds;
    }

    /**
     * @param rateLimitFrequencyInSeconds 限流时间窗口（秒）；仅当 rateLimitPerFrequency 大于 0 时生效
     */
    public void setRateLimitFrequencyInSeconds(Integer rateLimitFrequencyInSeconds) {
        this.rateLimitFrequencyInSeconds = rateLimitFrequencyInSeconds;
    }

    /**
     * @param concurrentExecLimit 允许同时处于 IN_PROGRESS 状态的最大任务数；设置为 0 表示移除限制
     */
    public void setConcurrentExecLimit(Integer concurrentExecLimit) {
        this.concurrentExecLimit = concurrentExecLimit;
    }

    /**
     * @return 允许同时处于 IN_PROGRESS 状态的最大任务数
     */
    public Integer getConcurrentExecLimit() {
        return concurrentExecLimit;
    }

    /**
     * @return 并发限制，未设置时返回 0（表示无限制）
     */
    public int concurrencyLimit() {
        return concurrentExecLimit == null ? 0 : concurrentExecLimit;
    }

    /**
     * @param inputTemplate 输入模板
     */
    public void setInputTemplate(Map<String, Object> inputTemplate) {
        this.inputTemplate = inputTemplate;
    }

    public String getIsolationGroupId() {
        return isolationGroupId;
    }

    public void setIsolationGroupId(String isolationGroupId) {
        this.isolationGroupId = isolationGroupId;
    }

    public String getExecutionNameSpace() {
        return executionNameSpace;
    }

    public void setExecutionNameSpace(String executionNameSpace) {
        this.executionNameSpace = executionNameSpace;
    }

    /**
     * @return 任务定义负责人的邮箱
     */
    public String getOwnerEmail() {
        return ownerEmail;
    }

    /**
     * @param ownerEmail 任务定义负责人的邮箱
     */
    public void setOwnerEmail(String ownerEmail) {
        this.ownerEmail = ownerEmail;
    }

    /**
     * @param pollTimeoutSeconds 轮询超时时间
     */
    public void setPollTimeoutSeconds(Integer pollTimeoutSeconds) {
        this.pollTimeoutSeconds = pollTimeoutSeconds;
    }

    /**
     * @return 任务定义的轮询超时时间
     */
    public Integer getPollTimeoutSeconds() {
        return pollTimeoutSeconds;
    }

    /**
     * @param backoffScaleFactor 退避比例因子
     */
    public void setBackoffScaleFactor(Integer backoffScaleFactor) {
        this.backoffScaleFactor = backoffScaleFactor;
    }

    /**
     * @return 任务定义的退避比例因子
     */
    public Integer getBackoffScaleFactor() {
        return backoffScaleFactor;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskDef taskDef = (TaskDef) o;
        return getRetryCount() == taskDef.getRetryCount()
                && getTimeoutSeconds() == taskDef.getTimeoutSeconds()
                && getRetryDelaySeconds() == taskDef.getRetryDelaySeconds()
                && getBackoffScaleFactor() == taskDef.getBackoffScaleFactor()
                && getResponseTimeoutSeconds() == taskDef.getResponseTimeoutSeconds()
                && Objects.equals(getName(), taskDef.getName())
                && Objects.equals(getDescription(), taskDef.getDescription())
                && Objects.equals(getInputKeys(), taskDef.getInputKeys())
                && Objects.equals(getOutputKeys(), taskDef.getOutputKeys())
                && getTimeoutPolicy() == taskDef.getTimeoutPolicy()
                && getRetryLogic() == taskDef.getRetryLogic()
                && Objects.equals(getConcurrentExecLimit(), taskDef.getConcurrentExecLimit())
                && Objects.equals(getRateLimitPerFrequency(), taskDef.getRateLimitPerFrequency())
                && Objects.equals(getInputTemplate(), taskDef.getInputTemplate())
                && Objects.equals(getIsolationGroupId(), taskDef.getIsolationGroupId())
                && Objects.equals(getExecutionNameSpace(), taskDef.getExecutionNameSpace())
                && Objects.equals(getOwnerEmail(), taskDef.getOwnerEmail());
    }

    @Override
    public int hashCode() {

        return Objects.hash(
                getName(),
                getDescription(),
                getRetryCount(),
                getTimeoutSeconds(),
                getInputKeys(),
                getOutputKeys(),
                getTimeoutPolicy(),
                getRetryLogic(),
                getRetryDelaySeconds(),
                getBackoffScaleFactor(),
                getResponseTimeoutSeconds(),
                getConcurrentExecLimit(),
                getRateLimitPerFrequency(),
                getInputTemplate(),
                getIsolationGroupId(),
                getExecutionNameSpace(),
                getOwnerEmail());
    }
}