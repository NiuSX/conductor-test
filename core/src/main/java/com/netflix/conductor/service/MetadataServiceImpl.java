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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.netflix.conductor.common.constraints.OwnerEmailMandatoryConstraint;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;
import com.netflix.conductor.common.model.BulkResponse;
import com.netflix.conductor.core.WorkflowContext;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.dao.EventHandlerDAO;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.validations.ValidationContext;

/**
 * MetadataService 的实现类：负责"定义"的增删改查。
 *
 * 它是定义管理的核心实现，直接委托给 MetadataDAO / EventHandlerDAO 做持久化，
 * 并在写入前补充元数据（createdBy、createTime、updatedBy、updateTime）。
 *
 * 与 WorkflowServiceImpl 的分工：
 * - 本类：管定义（TaskDef / WorkflowDef / EventHandler）
 * - WorkflowServiceImpl：管实例（启动、调度、生命周期）
 */
@Service
public class MetadataServiceImpl implements MetadataService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataServiceImpl.class);
    private final MetadataDAO metadataDAO;
    private final EventHandlerDAO eventHandlerDAO;

    public MetadataServiceImpl(
            MetadataDAO metadataDAO,
            EventHandlerDAO eventHandlerDAO,
            ConductorProperties properties) {
        this.metadataDAO = metadataDAO;
        this.eventHandlerDAO = eventHandlerDAO;

        // 初始化校验上下文（让自定义校验器能访问 metadataDAO）
        ValidationContext.initialize(metadataDAO);
        // 配置 ownerEmail 是否为必填
        OwnerEmailMandatoryConstraint.WorkflowTaskValidValidator.setOwnerEmailMandatory(
                properties.isOwnerEmailMandatory());
    }

    // ==================== 任务定义（TaskDef）====================

    /**
     * 批量注册任务定义。
     * 对每个定义补充 createdBy / createTime，并清空 updatedBy / updateTime（因为是新建）。
     */
    public void registerTaskDef(List<TaskDef> taskDefinitions) {
        for (TaskDef taskDefinition : taskDefinitions) {
            // 记录创建者（从线程上下文获取调用方）
            taskDefinition.setCreatedBy(WorkflowContext.get().getClientApp());
            taskDefinition.setCreateTime(System.currentTimeMillis());
            taskDefinition.setUpdatedBy(null);
            taskDefinition.setUpdateTime(null);

            metadataDAO.createTaskDef(taskDefinition);
        }
    }

    /**
     * 校验工作流定义。
     * 空实现：因为参数标注了 @Valid，调用此方法时 Spring 会自动触发校验。
     */
    @Override
    public void validateWorkflowDef(WorkflowDef workflowDef) {
        // 无需实现，@Valid 注解会自动校验
    }

    /**
     * 更新任务定义。
     * 先校验定义存在，再补充 updatedBy / updateTime 后写回。
     *
     * @throws NotFoundException 定义不存在
     */
    public void updateTaskDef(TaskDef taskDefinition) {
        TaskDef existing = metadataDAO.getTaskDef(taskDefinition.getName());
        if (existing == null) {
            throw new NotFoundException("No such task by name %s", taskDefinition.getName());
        }
        taskDefinition.setUpdatedBy(WorkflowContext.get().getClientApp());
        taskDefinition.setUpdateTime(System.currentTimeMillis());
        metadataDAO.updateTaskDef(taskDefinition);
    }

    /** 删除任务定义 */
    public void unregisterTaskDef(String taskType) {
        metadataDAO.removeTaskDef(taskType);
    }

    /** @return 所有已注册的任务定义 */
    public List<TaskDef> getTaskDefs() {
        return metadataDAO.getAllTaskDefs();
    }

    /**
     * 按任务类型查询任务定义。
     *
     * @throws NotFoundException 定义不存在
     */
    public TaskDef getTaskDef(String taskType) {
        TaskDef taskDef = metadataDAO.getTaskDef(taskType);
        if (taskDef == null) {
            throw new NotFoundException("No such taskType found by name: %s", taskType);
        }
        return taskDef;
    }

    // ==================== 工作流定义（WorkflowDef）====================

    /**
     * 更新工作流定义：补充 updateTime 后写回。
     */
    public void updateWorkflowDef(WorkflowDef workflowDef) {
        workflowDef.setUpdateTime(System.currentTimeMillis());
        metadataDAO.updateWorkflowDef(workflowDef);
    }

    /**
     * 批量更新工作流定义。
     * 逐个更新，成功/失败分别记录到 BulkResponse，不因单个失败中断整体。
     */
    public BulkResponse updateWorkflowDef(List<WorkflowDef> workflowDefList) {
        BulkResponse bulkResponse = new BulkResponse();
        for (WorkflowDef workflowDef : workflowDefList) {
            try {
                updateWorkflowDef(workflowDef);
                bulkResponse.appendSuccessResponse(workflowDef.getName());
            } catch (Exception e) {
                // 单个失败不影响其他，记录失败原因
                LOGGER.error("bulk update workflow def failed, name {} ", workflowDef.getName(), e);
                bulkResponse.appendFailedResponse(workflowDef.getName(), e.getMessage());
            }
        }
        return bulkResponse;
    }

    /**
     * 按名称和版本查询工作流定义。
     * version 为 null 时查最新版本。
     *
     * @throws NotFoundException 定义不存在
     */
    public WorkflowDef getWorkflowDef(String name, Integer version) {
        Optional<WorkflowDef> workflowDef;
        if (version == null) {
            workflowDef = metadataDAO.getLatestWorkflowDef(name);
        } else {
            workflowDef = metadataDAO.getWorkflowDef(name, version);
        }

        return workflowDef.orElseThrow(
                () ->
                        new NotFoundException(
                                "No such workflow found by name: %s, version: %d", name, version));
    }

    /**
     * 查询指定名称的最新版本工作流定义。
     *
     * @return 最新版本定义（可能为空）
     */
    public Optional<WorkflowDef> getLatestWorkflow(String name) {
        return metadataDAO.getLatestWorkflowDef(name);
    }

    /** @return 所有工作流定义（含所有版本） */
    public List<WorkflowDef> getWorkflowDefs() {
        return metadataDAO.getAllWorkflowDefs();
    }

    /**
     * 注册工作流定义：补充 createTime 后写入。
     */
    public void registerWorkflowDef(WorkflowDef workflowDef) {
        workflowDef.setCreateTime(System.currentTimeMillis());
        metadataDAO.createWorkflowDef(workflowDef);
    }

    /** 删除工作流定义（按名称 + 版本） */
    public void unregisterWorkflowDef(String name, Integer version) {
        metadataDAO.removeWorkflowDef(name, version);
    }

    // ==================== 事件处理器（EventHandler）====================

    /** 添加事件处理器（若同名已存在会抛异常，由 DAO 保证） */
    public void addEventHandler(EventHandler eventHandler) {
        eventHandlerDAO.addEventHandler(eventHandler);
    }

    /** 更新事件处理器 */
    public void updateEventHandler(EventHandler eventHandler) {
        eventHandlerDAO.updateEventHandler(eventHandler);
    }

    /** 移除事件处理器 */
    public void removeEventHandlerStatus(String name) {
        eventHandlerDAO.removeEventHandler(name);
    }

    /** @return 所有事件处理器 */
    public List<EventHandler> getAllEventHandlers() {
        return eventHandlerDAO.getAllEventHandlers();
    }

    /**
     * 按事件名查询事件处理器。
     *
     * @param event 事件名
     * @param activeOnly 是否只返回活跃的
     */
    public List<EventHandler> getEventHandlersForEvent(String event, boolean activeOnly) {
        return eventHandlerDAO.getEventHandlersForEvent(event, activeOnly);
    }

    /** @return 所有工作流定义的最新版本 */
    @Override
    public List<WorkflowDef> getWorkflowDefsLatestVersions() {
        return metadataDAO.getAllWorkflowDefsLatestVersions();
    }

    /**
     * 查询所有工作流的名称和版本（不含定义体）。
     * 结构：工作流名 → 该名称下所有版本的 TreeSet（按版本排序）。
     */
    public Map<String, ? extends Iterable<WorkflowDefSummary>> getWorkflowNamesAndVersions() {
        List<WorkflowDef> workflowDefs = metadataDAO.getAllWorkflowDefs();

        Map<String, TreeSet<WorkflowDefSummary>> retval = new HashMap<>();
        for (WorkflowDef def : workflowDefs) {
            String workflowName = def.getName();
            WorkflowDefSummary summary = fromWorkflowDef(def);

            // 该名称首次出现时初始化 TreeSet
            retval.putIfAbsent(workflowName, new TreeSet<WorkflowDefSummary>());

            TreeSet<WorkflowDefSummary> versions = retval.get(workflowName);
            versions.add(summary);
        }

        return retval;
    }

    /** 把 WorkflowDef 转成轻量的 WorkflowDefSummary（只含 name、version、createTime） */
    private WorkflowDefSummary fromWorkflowDef(WorkflowDef def) {
        WorkflowDefSummary summary = new WorkflowDefSummary();
        summary.setName(def.getName());
        summary.setVersion(def.getVersion());
        summary.setCreateTime(def.getCreateTime());

        return summary;
    }
}