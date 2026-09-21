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
package com.netflix.conductor.service;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.springframework.validation.annotation.Validated;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;
import com.netflix.conductor.common.model.BulkResponse;

/**
 * 元数据服务接口：管理 Conductor 的"定义"（Metadata），包括：
 * - 任务定义（TaskDef）
 * - 工作流定义（WorkflowDef）
 * - 事件处理器（EventHandler）
 *
 * 它与 WorkflowService 是并列关系，职责正好互补：
 * - MetadataService（本接口）：管"定义"——注册、更新、查询、删除
 * - WorkflowService：管"实例"——启动、调度、暂停、重试、终止
 *
 * 这种"定义与实例分离"是 Conductor 的核心设计，好处是定义可独立版本化，
 * 实例启动时对定义做快照，之后定义变更不影响已运行的实例。
 */
@Validated
public interface MetadataService {

    // ==================== 任务定义（TaskDef）====================

    /**
     * 批量注册任务定义。
     *
     * @param taskDefinitions 要注册的任务定义列表（不能为空，至少 1 个）
     */
    void registerTaskDef(
            @NotNull(message = "TaskDefList cannot be empty or null")
            @Size(min = 1, message = "TaskDefList is empty")
            List<@Valid TaskDef> taskDefinitions);

    /**
     * 更新任务定义。
     *
     * @param taskDefinition 要更新的任务定义
     */
    void updateTaskDef(@NotNull(message = "TaskDef cannot be null") @Valid TaskDef taskDefinition);

    /**
     * 删除（注销）任务定义。
     *
     * @param taskType 任务类型名
     */
    void unregisterTaskDef(@NotEmpty(message = "TaskName cannot be null or empty") String taskType);

    /**
     * @return 所有已注册的任务定义
     */
    List<TaskDef> getTaskDefs();

    /**
     * 按任务类型查询任务定义。
     *
     * @param taskType 任务类型名
     * @return 任务定义
     */
    TaskDef getTaskDef(@NotEmpty(message = "TaskType cannot be null or empty") String taskType);

    // ==================== 工作流定义（WorkflowDef）====================

    /**
     * 更新工作流定义。
     *
     * @param def 要更新的工作流定义
     */
    void updateWorkflowDef(@NotNull(message = "WorkflowDef cannot be null") @Valid WorkflowDef def);

    /**
     * 批量更新工作流定义。
     *
     * @param workflowDefList 要更新的工作流定义列表（不能为空，至少 1 个）
     * @return 批量操作结果
     */
    BulkResponse updateWorkflowDef(
            @NotNull(message = "WorkflowDef list name cannot be null or empty")
            @Size(min = 1, message = "WorkflowDefList is empty")
            List<@NotNull(message = "WorkflowDef cannot be null") @Valid WorkflowDef>
                    workflowDefList);

    /**
     * 按名称和版本查询工作流定义。
     *
     * @param name 工作流名称
     * @param version 版本；为 null 时返回最新版本
     * @return 工作流定义
     */
    WorkflowDef getWorkflowDef(
            @NotEmpty(message = "Workflow name cannot be null or empty") String name,
            Integer version);

    /**
     * 查询指定名称的最新版本工作流定义。
     *
     * @param name 工作流名称
     * @return 最新版本的工作流定义（可能为空）
     */
    Optional<WorkflowDef> getLatestWorkflow(
            @NotEmpty(message = "Workflow name cannot be null or empty") String name);

    /**
     * @return 所有工作流定义（含所有版本）
     */
    List<WorkflowDef> getWorkflowDefs();

    /**
     * @return 工作流名称和版本列表（不含定义体，用于轻量查询）
     */
    Map<String, ? extends Iterable<WorkflowDefSummary>> getWorkflowNamesAndVersions();

    /**
     * 注册工作流定义。
     *
     * @param workflowDef 要注册的工作流定义
     */
    void registerWorkflowDef(
            @NotNull(message = "WorkflowDef cannot be null") @Valid WorkflowDef workflowDef);

    /**
     * 校验工作流定义。
     * 默认空实现：因为参数已标注 @Valid，调用此方法时 Spring 会自动触发校验。
     *
     * @param workflowDef 要校验的工作流定义
     */
    default void validateWorkflowDef(
            @NotNull(message = "WorkflowDef cannot be null") @Valid WorkflowDef workflowDef) {
        // 无需实现，@Valid 注解会在调用时自动校验
    }

    /**
     * 删除（注销）工作流定义。
     *
     * @param name 工作流名称
     * @param version 版本
     */
    void unregisterWorkflowDef(
            @NotEmpty(message = "Workflow name cannot be null or empty") String name,
            @NotNull(message = "Version cannot be null") Integer version);

    // ==================== 事件处理器（EventHandler）====================

    /**
     * 添加事件处理器。若同名已存在则抛异常。
     *
     * @param eventHandler 要添加的事件处理器
     */
    void addEventHandler(
            @NotNull(message = "EventHandler cannot be null") @Valid EventHandler eventHandler);

    /**
     * 更新事件处理器。
     *
     * @param eventHandler 要更新的事件处理器
     */
    void updateEventHandler(
            @NotNull(message = "EventHandler cannot be null") @Valid EventHandler eventHandler);

    /**
     * 按名称移除事件处理器。
     *
     * @param name 事件名称
     */
    void removeEventHandlerStatus(
            @NotEmpty(message = "EventName cannot be null or empty") String name);

    /**
     * @return 系统中注册的所有事件处理器
     */
    List<EventHandler> getAllEventHandlers();

    /**
     * 按事件名查询事件处理器。
     *
     * @param event 事件名
     * @param activeOnly 是否只返回活跃的处理器
     * @return 事件处理器列表
     */
    List<EventHandler> getEventHandlersForEvent(
            @NotEmpty(message = "EventName cannot be null or empty") String event,
            boolean activeOnly);

    /**
     * @return 所有工作流定义的最新版本列表
     */
    List<WorkflowDef> getWorkflowDefsLatestVersions();
}