/*
 * Copyright 2020 Netflix, Inc.
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
package com.netflix.conductor.common.run;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.netflix.conductor.annotations.protogen.ProtoField;
import com.netflix.conductor.annotations.protogen.ProtoMessage;
import com.netflix.conductor.common.run.Workflow.WorkflowStatus;
import com.netflix.conductor.common.utils.SummaryUtil;

/**
 * 捕获工作流摘要信息，用于在 Elastic Search 中建立索引。
 * 该类是工作流（Workflow）的轻量级摘要表示，只包含用于搜索和展示的关键字段，
 * 避免将完整的工作流对象（包含大量任务详情）写入索引。
 */
@ProtoMessage
public class WorkflowSummary {

    /** 时间应以 GMT 时区存储 */
    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

    /** 工作流类型（即工作流名称） */
    @ProtoField(id = 1)
    private String workflowType;

    /** 工作流版本号 */
    @ProtoField(id = 2)
    private int version;

    /** 工作流唯一标识 ID */
    @ProtoField(id = 3)
    private String workflowId;

    /** 关联 ID，用于将多个工作流关联在一起 */
    @ProtoField(id = 4)
    private String correlationId;

    /** 工作流开始时间（格式化后的字符串） */
    @ProtoField(id = 5)
    private String startTime;

    /** 工作流更新时间（格式化后的字符串） */
    @ProtoField(id = 6)
    private String updateTime;

    /** 工作流结束时间（格式化后的字符串） */
    @ProtoField(id = 7)
    private String endTime;

    /** 工作流当前状态 */
    @ProtoField(id = 8)
    private Workflow.WorkflowStatus status;

    /** 工作流输入参数（序列化后的字符串） */
    @ProtoField(id = 9)
    private String input;

    /** 工作流输出结果（序列化后的字符串） */
    @ProtoField(id = 10)
    private String output;

    /** 工作流未完成的原因（如失败原因） */
    @ProtoField(id = 11)
    private String reasonForIncompletion;

    /** 工作流执行耗时（毫秒） */
    @ProtoField(id = 12)
    private long executionTime;

    /** 触发工作流的事件 */
    @ProtoField(id = 13)
    private String event;

    /** 失败的任务引用名称（逗号分隔的字符串） */
    @ProtoField(id = 14)
    private String failedReferenceTaskNames = "";

    /** 外部输入负载存储路径（当输入过大时存储到外部） */
    @ProtoField(id = 15)
    private String externalInputPayloadStoragePath;

    /** 外部输出负载存储路径（当输出过大时存储到外部） */
    @ProtoField(id = 16)
    private String externalOutputPayloadStoragePath;

    /** 任务优先级（0-99） */
    @ProtoField(id = 17)
    private int priority;

    /** 失败的任务名称集合 */
    @ProtoField(id = 18)
    private Set<String> failedTaskNames = new HashSet<>();

    /** 默认构造函数 */
    public WorkflowSummary() {}

    /**
     * 通过 Workflow 对象构造 WorkflowSummary。
     * 从完整的工作流对象中提取关键字段，并将时间格式化为 GMT 时区的 ISO8601 字符串。
     *
     * @param workflow 完整的工作流对象
     */
    public WorkflowSummary(Workflow workflow) {

        // 时间格式化器，使用 ISO8601 格式并以 GMT 时区表示
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        sdf.setTimeZone(GMT);

        this.workflowType = workflow.getWorkflowName();
        this.version = workflow.getWorkflowVersion();
        this.workflowId = workflow.getWorkflowId();
        this.priority = workflow.getPriority();
        this.correlationId = workflow.getCorrelationId();
        // 如果创建时间不为空，则格式化为开始时间
        if (workflow.getCreateTime() != null) {
            this.startTime = sdf.format(new Date(workflow.getCreateTime()));
        }
        // 如果结束时间大于 0，则格式化为结束时间
        if (workflow.getEndTime() > 0) {
            this.endTime = sdf.format(new Date(workflow.getEndTime()));
        }
        // 如果更新时间不为空，则格式化为更新时间
        if (workflow.getUpdateTime() != null) {
            this.updateTime = sdf.format(new Date(workflow.getUpdateTime()));
        }
        this.status = workflow.getStatus();
        // 如果输入不为空，则序列化输入
        if (workflow.getInput() != null) {
            this.input = SummaryUtil.serializeInputOutput(workflow.getInput());
        }
        // 如果输出不为空，则序列化输出
        if (workflow.getOutput() != null) {
            this.output = SummaryUtil.serializeInputOutput(workflow.getOutput());
        }
        this.reasonForIncompletion = workflow.getReasonForIncompletion();
        // 如果结束时间大于 0，则计算执行耗时
        if (workflow.getEndTime() > 0) {
            this.executionTime = workflow.getEndTime() - workflow.getStartTime();
        }
        this.event = workflow.getEvent();
        // 将失败的任务引用名称集合用逗号连接成字符串
        this.failedReferenceTaskNames =
                workflow.getFailedReferenceTaskNames().stream().collect(Collectors.joining(","));
        this.failedTaskNames = workflow.getFailedTaskNames();
        // 如果外部输入负载存储路径不为空，则设置
        if (StringUtils.isNotBlank(workflow.getExternalInputPayloadStoragePath())) {
            this.externalInputPayloadStoragePath = workflow.getExternalInputPayloadStoragePath();
        }
        // 如果外部输出负载存储路径不为空，则设置
        if (StringUtils.isNotBlank(workflow.getExternalOutputPayloadStoragePath())) {
            this.externalOutputPayloadStoragePath = workflow.getExternalOutputPayloadStoragePath();
        }
    }

    /**
     * @return 工作流类型
     */
    public String getWorkflowType() {
        return workflowType;
    }

    /**
     * @return 版本号
     */
    public int getVersion() {
        return version;
    }

    /**
     * @return 工作流 ID
     */
    public String getWorkflowId() {
        return workflowId;
    }

    /**
     * @return 关联 ID
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * @return 开始时间
     */
    public String getStartTime() {
        return startTime;
    }

    /**
     * @return 结束时间
     */
    public String getEndTime() {
        return endTime;
    }

    /**
     * @return 工作流状态
     */
    public WorkflowStatus getStatus() {
        return status;
    }

    /**
     * @return 输入
     */
    public String getInput() {
        return input;
    }

    /**
     * @return 输入大小（字符数）
     */
    public long getInputSize() {
        return input != null ? input.length() : 0;
    }

    /**
     * @return 输出
     */
    public String getOutput() {
        return output;
    }

    /**
     * @return 输出大小（字符数）
     */
    public long getOutputSize() {
        return output != null ? output.length() : 0;
    }

    /**
     * @return 未完成原因
     */
    public String getReasonForIncompletion() {
        return reasonForIncompletion;
    }

    /**
     * @return 执行耗时
     */
    public long getExecutionTime() {
        return executionTime;
    }

    /**
     * @return 更新时间
     */
    public String getUpdateTime() {
        return updateTime;
    }

    /**
     * @return 事件
     */
    public String getEvent() {
        return event;
    }

    /**
     * @param event 事件
     */
    public void setEvent(String event) {
        this.event = event;
    }

    /**
     * @return 失败的任务引用名称（逗号分隔）
     */
    public String getFailedReferenceTaskNames() {
        return failedReferenceTaskNames;
    }

    /**
     * @param failedReferenceTaskNames 失败的任务引用名称
     */
    public void setFailedReferenceTaskNames(String failedReferenceTaskNames) {
        this.failedReferenceTaskNames = failedReferenceTaskNames;
    }

    /**
     * @return 失败的任务名称集合
     */
    public Set<String> getFailedTaskNames() {
        return failedTaskNames;
    }

    /**
     * @param failedTaskNames 失败的任务名称集合
     */
    public void setFailedTaskNames(Set<String> failedTaskNames) {
        this.failedTaskNames = failedTaskNames;
    }

    /**
     * @param workflowType 工作流类型
     */
    public void setWorkflowType(String workflowType) {
        this.workflowType = workflowType;
    }

    /**
     * @param version 版本号
     */
    public void setVersion(int version) {
        this.version = version;
    }

    /**
     * @param workflowId 工作流 ID
     */
    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    /**
     * @param correlationId 关联 ID
     */
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * @param startTime 开始时间
     */
    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    /**
     * @param updateTime 更新时间
     */
    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    /**
     * @param endTime 结束时间
     */
    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    /**
     * @param status 工作流状态
     */
    public void setStatus(WorkflowStatus status) {
        this.status = status;
    }

    /**
     * @param input 输入
     */
    public void setInput(String input) {
        this.input = input;
    }

    /**
     * @param output 输出
     */
    public void setOutput(String output) {
        this.output = output;
    }

    /**
     * @param reasonForIncompletion 未完成原因
     */
    public void setReasonForIncompletion(String reasonForIncompletion) {
        this.reasonForIncompletion = reasonForIncompletion;
    }

    /**
     * @param executionTime 执行耗时
     */
    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    /**
     * @return 工作流输入负载的外部存储路径
     */
    public String getExternalInputPayloadStoragePath() {
        return externalInputPayloadStoragePath;
    }

    /**
     * @param externalInputPayloadStoragePath 工作流输入负载存储的外部路径
     */
    public void setExternalInputPayloadStoragePath(String externalInputPayloadStoragePath) {
        this.externalInputPayloadStoragePath = externalInputPayloadStoragePath;
    }

    /**
     * @return 工作流输出负载的外部存储路径
     */
    public String getExternalOutputPayloadStoragePath() {
        return externalOutputPayloadStoragePath;
    }

    /**
     * @param externalOutputPayloadStoragePath 工作流输出负载存储的外部路径
     */
    public void setExternalOutputPayloadStoragePath(String externalOutputPayloadStoragePath) {
        this.externalOutputPayloadStoragePath = externalOutputPayloadStoragePath;
    }

    /**
     * @return 任务优先级
     */
    public int getPriority() {
        return priority;
    }

    /**
     * @param priority 任务优先级（0 到 99 之间）
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }

    /**
     * 判断两个 WorkflowSummary 对象是否相等。
     * 比较版本号、执行耗时、优先级、工作流类型、工作流 ID、关联 ID、
     * 开始/更新/结束时间、状态、未完成原因和事件等字段。
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WorkflowSummary that = (WorkflowSummary) o;
        return getVersion() == that.getVersion()
                && getExecutionTime() == that.getExecutionTime()
                && getPriority() == that.getPriority()
                && getWorkflowType().equals(that.getWorkflowType())
                && getWorkflowId().equals(that.getWorkflowId())
                && Objects.equals(getCorrelationId(), that.getCorrelationId())
                && StringUtils.equals(getStartTime(), that.getStartTime())
                && StringUtils.equals(getUpdateTime(), that.getUpdateTime())
                && StringUtils.equals(getEndTime(), that.getEndTime())
                && getStatus() == that.getStatus()
                && Objects.equals(getReasonForIncompletion(), that.getReasonForIncompletion())
                && Objects.equals(getEvent(), that.getEvent());
    }

    /**
     * 计算 WorkflowSummary 对象的哈希值。
     * 基于工作流类型、版本号、工作流 ID、关联 ID、开始/更新/结束时间、
     * 状态、未完成原因、执行耗时、事件和优先级等字段计算。
     */
    @Override
    public int hashCode() {
        return Objects.hash(
                getWorkflowType(),
                getVersion(),
                getWorkflowId(),
                getCorrelationId(),
                getStartTime(),
                getUpdateTime(),
                getEndTime(),
                getStatus(),
                getReasonForIncompletion(),
                getExecutionTime(),
                getEvent(),
                getPriority());
    }
}