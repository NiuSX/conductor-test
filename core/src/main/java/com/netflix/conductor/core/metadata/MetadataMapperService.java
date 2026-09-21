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
package com.netflix.conductor.core.metadata;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.annotations.VisibleForTesting;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.SubWorkflowParams;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.WorkflowContext;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.core.utils.Utils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/**
 * 元数据填充服务：把"定义"（TaskDef、WorkflowDef、子工作流版本）填充到"运行对象"里。
 *
 * 核心作用：在工作流启动前，把定义"冻结"到运行时对象中，带来两个好处：
 * <ul>
 *   <li>定义在运行期间不可变，保证运行时一致性（定义后来改了也不影响已启动的实例）
 *   <li>减少对存储层的压力（运行时不必反复查定义）
 * </ul>
 *
 * 它是"定义与实例分离"架构的关键粘合层：
 * - MetadataService/DAO 管定义的持久化
 * - 本类负责在启动/需要时把定义"快照"进 WorkflowModel / TaskModel
 */
@Component
public class MetadataMapperService {

    public static final Logger LOGGER = LoggerFactory.getLogger(MetadataMapperService.class);
    private final MetadataDAO metadataDAO;

    public MetadataMapperService(MetadataDAO metadataDAO) {
        this.metadataDAO = metadataDAO;
    }

    // ==================== 查询工作流定义 ====================

    /**
     * 按名称和版本查找工作流定义。version 为 null 时查最新版本。
     *
     * @throws NotFoundException 找不到定义
     */
    public WorkflowDef lookupForWorkflowDefinition(String name, Integer version) {
        Optional<WorkflowDef> potentialDef =
                version == null
                        ? lookupLatestWorkflowDefinition(name)
                        : lookupWorkflowDefinition(name, version);

        // 校验定义是否存在
        return potentialDef.orElseThrow(
                () -> {
                    LOGGER.error(
                            "There is no workflow defined with name {} and version {}",
                            name,
                            version);
                    return new NotFoundException(
                            "No such workflow defined. name=%s, version=%s", name, version);
                });
    }

    /** 按名称 + 版本查定义（包可见，便于测试） */
    @VisibleForTesting
    Optional<WorkflowDef> lookupWorkflowDefinition(String workflowName, int workflowVersion) {
        Utils.checkArgument(
                StringUtils.isNotBlank(workflowName),
                "Workflow name must be specified when searching for a definition");
        return metadataDAO.getWorkflowDef(workflowName, workflowVersion);
    }

    /** 按名称查最新版本定义（包可见，便于测试） */
    @VisibleForTesting
    Optional<WorkflowDef> lookupLatestWorkflowDefinition(String workflowName) {
        Utils.checkArgument(
                StringUtils.isNotBlank(workflowName),
                "Workflow name must be specified when searching for a definition");
        return metadataDAO.getLatestWorkflowDef(workflowName);
    }

    // ==================== 填充工作流定义 ====================

    /**
     * 把工作流定义（含各任务的 TaskDef、子工作流版本）填充到 WorkflowModel。
     *
     * 流程：
     * 1. 若 WorkflowModel 没有定义，按 name + version 查并设置
     * 2. 遍历定义中所有 WorkflowTask，逐个填充 TaskDef
     * 3. 校验 SIMPLE 任务的 TaskDef 是否齐全（不齐则抛异常）
     */
    public WorkflowModel populateWorkflowWithDefinitions(WorkflowModel workflow) {
        Utils.checkNotNull(workflow, "workflow cannot be null");
        // 若运行对象没有定义，则查库补上
        WorkflowDef workflowDefinition =
                Optional.ofNullable(workflow.getWorkflowDefinition())
                        .orElseGet(
                                () -> {
                                    WorkflowDef wd =
                                            lookupForWorkflowDefinition(
                                                    workflow.getWorkflowName(),
                                                    workflow.getWorkflowVersion());
                                    workflow.setWorkflowDefinition(wd);
                                    return wd;
                                });

        // 收集所有 WorkflowTask，逐个填充 TaskDef
        workflowDefinition.collectTasks().forEach(this::populateWorkflowTaskWithDefinition);
        // 校验 SIMPLE 任务的 TaskDef 是否齐全
        checkNotEmptyDefinitions(workflowDefinition);

        return workflow;
    }

    /**
     * 把 TaskDef 和子工作流版本填充到 WorkflowDef（不涉及 WorkflowModel）。
     * 用于在工作流定义发布/校验时预填充。
     */
    public WorkflowDef populateTaskDefinitions(WorkflowDef workflowDefinition) {
        Utils.checkNotNull(workflowDefinition, "workflowDefinition cannot be null");
        workflowDefinition.collectTasks().forEach(this::populateWorkflowTaskWithDefinition);
        checkNotEmptyDefinitions(workflowDefinition);
        return workflowDefinition;
    }

    /**
     * 填充单个 WorkflowTask 的定义：
     * - 若需要（无 TaskDef 且有 name），查库设置 TaskDef
     * - SIMPLE 任务若查不到 TaskDef，创建 ad-hoc 的 TaskDef
     * - SUB_WORKFLOW 任务则填充子工作流版本
     */
    private void populateWorkflowTaskWithDefinition(WorkflowTask workflowTask) {
        Utils.checkNotNull(workflowTask, "WorkflowTask cannot be null");
        if (shouldPopulateTaskDefinition(workflowTask)) {
            workflowTask.setTaskDefinition(metadataDAO.getTaskDef(workflowTask.getName()));
            if (workflowTask.getTaskDefinition() == null
                    && workflowTask.getType().equals(TaskType.SIMPLE.name())) {
                // 临时任务定义：SIMPLE 任务允许没有预注册的 TaskDef，用默认值兜底
                workflowTask.setTaskDefinition(new TaskDef(workflowTask.getName()));
            }
        }
        // SUB_WORKFLOW 任务：填充子工作流版本（版本为空时取最新）
        if (workflowTask.getType().equals(TaskType.SUB_WORKFLOW.name())) {
            populateVersionForSubWorkflow(workflowTask);
        }
    }

    /**
     * 为 SUB_WORKFLOW 任务填充子工作流版本。
     * 若子工作流参数未指定版本，则查该子工作流的最新版本并设置。
     *
     * @throws TerminateWorkflowException 子工作流定义不存在
     */
    private void populateVersionForSubWorkflow(WorkflowTask workflowTask) {
        Utils.checkNotNull(workflowTask, "WorkflowTask cannot be null");
        SubWorkflowParams subworkflowParams = workflowTask.getSubWorkflowParam();
        if (subworkflowParams.getVersion() == null) {
            String subWorkflowName = subworkflowParams.getName();
            // 查子工作流最新版本
            Integer subWorkflowVersion =
                    metadataDAO
                            .getLatestWorkflowDef(subWorkflowName)
                            .map(WorkflowDef::getVersion)
                            .orElseThrow(
                                    () -> {
                                        String reason =
                                                String.format(
                                                        "The Task %s defined as a sub-workflow has no workflow definition available ",
                                                        subWorkflowName);
                                        LOGGER.error(reason);
                                        return new TerminateWorkflowException(reason);
                                    });
            subworkflowParams.setVersion(subWorkflowVersion);
        }
    }

    /**
     * 校验所有 SIMPLE 任务都有 TaskDef。
     * 收集缺失定义的 SIMPLE 任务名，若非空则抛异常并记录指标。
     */
    private void checkNotEmptyDefinitions(WorkflowDef workflowDefinition) {
        Utils.checkNotNull(workflowDefinition, "WorkflowDefinition cannot be null");

        // 收集缺失定义的 SIMPLE 任务名
        Set<String> missingTaskDefinitionNames =
                workflowDefinition.collectTasks().stream()
                        .filter(
                                workflowTask ->
                                        workflowTask.getType().equals(TaskType.SIMPLE.name()))
                        .filter(this::shouldPopulateTaskDefinition)
                        .map(WorkflowTask::getName)
                        .collect(Collectors.toSet());

        if (!missingTaskDefinitionNames.isEmpty()) {
            LOGGER.error(
                    "Cannot find the task definitions for the following tasks used in workflow: {}",
                    missingTaskDefinitionNames);
            Monitors.recordWorkflowStartError(
                    workflowDefinition.getName(), WorkflowContext.get().getClientApp());
            throw new IllegalArgumentException(
                    "Cannot find the task definitions for the following tasks used in workflow: "
                            + missingTaskDefinitionNames);
        }
    }

    // ==================== 填充任务定义 ====================

    /**
     * 把 TaskDef 填充到 TaskModel（通过其关联的 WorkflowTask）。
     * 用于按 taskId 查询任务时，保证任务定义完整。
     */
    public TaskModel populateTaskWithDefinition(TaskModel task) {
        Utils.checkNotNull(task, "Task cannot be null");
        populateWorkflowTaskWithDefinition(task.getWorkflowTask());
        return task;
    }

    /**
     * 判断某个 WorkflowTask 是否需要填充 TaskDef：
     * 条件是该任务当前没有 TaskDef，且有非空的 name。
     */
    @VisibleForTesting
    boolean shouldPopulateTaskDefinition(WorkflowTask workflowTask) {
        Utils.checkNotNull(workflowTask, "WorkflowTask cannot be null");
        Utils.checkNotNull(workflowTask.getType(), "WorkflowTask type cannot be null");
        return workflowTask.getTaskDefinition() == null
                && StringUtils.isNotBlank(workflowTask.getName());
    }
}