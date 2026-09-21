/*
 * Copyright 2023 Netflix, Inc.
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
package com.netflix.conductor.core.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.model.TaskModel;

/**
 * TaskStatusListener 的桩（Stub）实现：默认实现，只记录 debug 日志。
 *
 * 它的作用是：
 * - 作为默认的 TaskStatusListener 注入到引擎里，保证没有自定义监听器时也能正常工作
 * - 把每个状态变更以 debug 级别记录下来，便于排查问题
 * - 不产生任何副作用（不发通知、不改状态），是"安全"的默认实现
 *
 * 使用者可以替换成自己的实现（如发告警、写审计日志），
 * 而不必继承本类或实现全部方法。
 */
public class TaskStatusListenerStub implements TaskStatusListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskStatusListenerStub.class);

    /** 任务被调度时记录 debug 日志 */
    @Override
    public void onTaskScheduled(TaskModel task) {
        LOGGER.debug("Task {} is scheduled", task.getTaskId());
    }

    /** 任务被取消时记录 debug 日志 */
    @Override
    public void onTaskCanceled(TaskModel task) {
        LOGGER.debug("Task {} is canceled", task.getTaskId());
    }

    /** 任务成功完成时记录 debug 日志 */
    @Override
    public void onTaskCompleted(TaskModel task) {
        LOGGER.debug("Task {} is completed", task.getTaskId());
    }

    /** 任务完成但有错误时记录 debug 日志 */
    @Override
    public void onTaskCompletedWithErrors(TaskModel task) {
        LOGGER.debug("Task {} is completed with errors", task.getTaskId());
    }

    /** 任务失败时记录 debug 日志 */
    @Override
    public void onTaskFailed(TaskModel task) {
        LOGGER.debug("Task {} is failed", task.getTaskId());
    }

    /** 任务失败且不可重试时记录 debug 日志 */
    @Override
    public void onTaskFailedWithTerminalError(TaskModel task) {
        LOGGER.debug("Task {} is failed with terminal error", task.getTaskId());
    }

    /** 任务进入进行中时记录 debug 日志 */
    @Override
    public void onTaskInProgress(TaskModel task) {
        LOGGER.debug("Task {} is in-progress", task.getTaskId());
    }

    /** 任务被跳过时记录 debug 日志 */
    @Override
    public void onTaskSkipped(TaskModel task) {
        LOGGER.debug("Task {} is skipped", task.getTaskId());
    }

    /** 任务超时时记录 debug 日志 */
    @Override
    public void onTaskTimedOut(TaskModel task) {
        LOGGER.debug("Task {} is timed out", task.getTaskId());
    }
}