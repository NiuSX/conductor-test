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
package com.netflix.conductor.core.execution.tasks;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

/**
 * 系统任务注册表：一个容器类，保存"系统任务类型"到 {@link WorkflowSystemTask} 实例的映射。
 *
 * 在 Conductor 中，系统任务（如 Wait、Switch、Join、SubWorkflow、HTTP 等）是引擎内置的，
 * 不需要外部 Worker 执行。这个注册表让引擎在运行时能根据 taskType 找到对应的系统任务实现，
 * 从而决定是"同步执行"还是"入队由 Worker 执行"。
 *
 * 它是通过 Spring 自动注入的：所有标注了 {@code @Component} 的 WorkflowSystemTask 子类
 * 都会被 Spring 收集到构造器的 {@code Set<WorkflowSystemTask> tasks} 参数中。
 */
@Component
public class SystemTaskRegistry {

    /** 异步系统任务的 Spring 限定符（用于按类型注入异步系统任务集合） */
    public static final String ASYNC_SYSTEM_TASKS_QUALIFIER = "asyncSystemTasks";

    /** 注册表：key = 任务类型（如 "WAIT"、"SWITCH"），value = 对应的系统任务实例 */
    private final Map<String, WorkflowSystemTask> registry;

    /**
     * 构造器：Spring 会把所有 WorkflowSystemTask 实现注入进来，
     * 按 taskType 建立映射。
     *
     * @param tasks 所有系统任务实现（由 Spring 自动收集）
     */
    public SystemTaskRegistry(Set<WorkflowSystemTask> tasks) {
        this.registry =
                tasks.stream()
                        .collect(
                                Collectors.toMap(
                                        WorkflowSystemTask::getTaskType, Function.identity()));
    }

    /**
     * 按任务类型获取对应的系统任务实例。
     *
     * @param taskType 任务类型
     * @return 对应的 WorkflowSystemTask
     * @throws IllegalStateException 如果该类型未注册
     */
    public WorkflowSystemTask get(String taskType) {
        return Optional.ofNullable(registry.get(taskType))
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        taskType + "not found in " + getClass().getSimpleName()));
    }

    /**
     * 判断某个任务类型是否为系统任务。
     * 引擎据此决定：系统任务由引擎内部处理（同步或异步），非系统任务入队交给 Worker。
     *
     * @param taskType 任务类型
     * @return true 表示是系统任务
     */
    public boolean isSystemTask(String taskType) {
        return registry.containsKey(taskType);
    }
}