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

import java.util.List;

import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.model.TaskModel;

/**
 * 任务映射器接口：把"工作流任务定义（WorkflowTask）"映射为"具体任务实例（TaskModel 列表）"。
 *
 * 这是 Conductor 中"定义 → 实例"转换的关键抽象。每种任务类型都有自己的 TaskMapper 实现：
 * - SimpleTaskMapper：SIMPLE 任务
 * - SwitchTaskMapper：SWITCH 任务（条件分支）
 * - JoinTaskMapper：JOIN 任务（并行汇合）
 * - DoWhileTaskMapper：DO_WHILE 任务（循环）
 * - SubWorkflowTaskMapper：SUB_WORKFLOW 任务
 * - ForkJoinTaskMapper：FORK_JOIN 任务
 * - 等等
 *
 * 为什么是"列表"而不是单个任务？
 * 因为有些任务类型（如 FORK_JOIN）在映射时会生成多个任务实例（每个分支一个），
 * 甚至还会生成配套的任务（如 FORK_JOIN 会同时生成 JOIN）。
 *
 * 它在 DeciderService 中被调用：
 * - getTasksToBeScheduled() 中通过 taskMappers.get(type) 找到对应 mapper
 * - 调用 getMappedTasks() 得到待调度的任务列表
 *
 * 与 SystemTaskRegistry 的区别：
 * - SystemTaskRegistry：管"任务执行"（start/execute/cancel），面向运行时
 * - TaskMapper（本接口）：管"任务映射"（定义 → 实例），面向调度前
 */
public interface TaskMapper {

    /**
     * 返回该 mapper 处理的任务类型（如 "SIMPLE"、"SWITCH"）。
     * 用于在 taskMappersByTaskType 映射中作为 key，按类型找到对应 mapper。
     *
     * @return 任务类型
     */
    String getTaskType();

    /**
     * 把工作流任务定义映射为具体的任务实例列表。
     *
     * @param taskMapperContext 映射上下文，包含工作流、任务定义、输入参数、
     *                          重试次数、taskId 等所有映射所需信息
     * @return 映射出的任务实例列表（可能多个）
     * @throws TerminateWorkflowException 映射过程中出现不可恢复的错误时抛出，
     *         会导致工作流终止
     */
    List<TaskModel> getMappedTasks(TaskMapperContext taskMapperContext)
            throws TerminateWorkflowException;
}