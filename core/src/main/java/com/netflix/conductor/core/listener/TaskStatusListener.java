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

import com.netflix.conductor.model.TaskModel;

/**
 * 任务状态变更监听器：用于在任务状态发生变化时收到回调。
 *
 * 设计要点：
 * - 所有方法都有默认空实现（default），实现类可以只覆盖自己关心的状态，
 *   而不必实现全部方法。
 * - 这是典型的"观察者模式 + 缺省适配器"设计，避免接口膨胀导致的空实现样板代码。
 *
 * 典型用途：
 * - 审计日志：记录任务状态流转
 * - 监控告警：任务失败/超时时发通知
 * - 指标统计：统计各状态的分布
 * - 外部系统集成：任务完成时触发下游动作
 *
 * 它在 Conductor 中的触发点：
 * - WorkflowExecutor.updateTask() 中调用 notifyTaskStatusListener(task)
 * - WorkflowExecutor.scheduleTask() 中调用 onTaskScheduled
 */
public interface TaskStatusListener {

    /** 任务被调度（SCHEDULED）时触发 */
    default void onTaskScheduled(TaskModel task) {}

    /** 任务进入进行中（IN_PROGRESS）时触发 */
    default void onTaskInProgress(TaskModel task) {}

    /** 任务被取消（CANCELED）时触发 */
    default void onTaskCanceled(TaskModel task) {}

    /** 任务失败（FAILED）时触发 */
    default void onTaskFailed(TaskModel task) {}

    /** 任务失败且不可重试（FAILED_WITH_TERMINAL_ERROR）时触发 */
    default void onTaskFailedWithTerminalError(TaskModel task) {}

    /** 任务成功完成（COMPLETED）时触发 */
    default void onTaskCompleted(TaskModel task) {}

    /** 任务完成但有错误（COMPLETED_WITH_ERRORS）时触发 */
    default void onTaskCompletedWithErrors(TaskModel task) {}

    /** 任务超时（TIMED_OUT）时触发 */
    default void onTaskTimedOut(TaskModel task) {}

    /** 任务被跳过（SKIPPED）时触发 */
    default void onTaskSkipped(TaskModel task) {}
}