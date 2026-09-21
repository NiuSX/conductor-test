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
package com.netflix.conductor.common.metadata.tasks;

import java.util.Objects;

import com.netflix.conductor.annotations.protogen.ProtoField;
import com.netflix.conductor.annotations.protogen.ProtoMessage;

/**
 * 任务执行日志模型。
 *
 * <p>表示任务（{@link Task}）在执行过程中产生的一条日志记录，用于追踪和排查任务执行情况。
 * 每条日志关联一个任务 ID，并记录日志内容与创建时间。
 *
 * <p>通过 {@link ProtoMessage} 和 {@link ProtoField} 注解支持 Protobuf 序列化。
 */
@ProtoMessage
public class TaskExecLog {

    /** 日志内容 */
    @ProtoField(id = 1)
    private String log;

    /** 关联的任务 ID */
    @ProtoField(id = 2)
    private String taskId;

    /** 日志创建时间（毫秒时间戳） */
    @ProtoField(id = 3)
    private long createdTime;

    public TaskExecLog() {}

    /**
     * 使用日志内容构造日志对象，并自动设置创建时间为当前时间。
     *
     * @param log 日志内容
     */
    public TaskExecLog(String log) {
        this.log = log;
        this.createdTime = System.currentTimeMillis();
    }

    /**
     * @return 任务执行日志内容
     */
    public String getLog() {
        return log;
    }

    /**
     * @param log 日志内容
     */
    public void setLog(String log) {
        this.log = log;
    }

    /**
     * @return 关联的任务 ID
     */
    public String getTaskId() {
        return taskId;
    }

    /**
     * @param taskId 关联的任务 ID
     */
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    /**
     * @return 日志创建时间（毫秒时间戳）
     */
    public long getCreatedTime() {
        return createdTime;
    }

    /**
     * @param createdTime 日志创建时间（毫秒时间戳）
     */
    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskExecLog that = (TaskExecLog) o;
        return createdTime == that.createdTime
                && Objects.equals(log, that.log)
                && Objects.equals(taskId, that.taskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(log, taskId, createdTime);
    }
}