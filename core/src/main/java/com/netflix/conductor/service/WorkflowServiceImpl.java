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

import org.springframework.stereotype.Service;

import com.netflix.conductor.annotations.Audit;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.SkipTaskRequest;
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.ExternalStorageLocation;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.core.execution.StartWorkflowInput;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.operation.StartWorkflowOperation;
import com.netflix.conductor.core.utils.Utils;

/**
 * WorkflowService 的实现类：工作流实例管理的门面实现。
 *
 * 它的角色是"薄封装 + 委托"：
 * - 启动相关 → 委托给 StartWorkflowOperation
 * - 生命周期操作 → 委托给 WorkflowExecutor
 * - 查询/搜索 → 委托给 ExecutionService
 * - 定义查询 → 委托给 MetadataService
 *
 * 它自身不做业务逻辑，只负责编排调用、参数组装和异常转换。
 *
 * @Audit 记录审计日志
 * @Trace 记录调用链追踪
 */
@Audit
@Trace
@Service
public class WorkflowServiceImpl implements WorkflowService {

    private final WorkflowExecutor workflowExecutor;
    private final ExecutionService executionService;
    private final MetadataService metadataService;
    private final StartWorkflowOperation startWorkflowOperation;

    public WorkflowServiceImpl(
            WorkflowExecutor workflowExecutor,
            ExecutionService executionService,
            MetadataService metadataService,
            StartWorkflowOperation startWorkflowOperation) {
        this.workflowExecutor = workflowExecutor;
        this.executionService = executionService;
        this.metadataService = metadataService;
        this.startWorkflowOperation = startWorkflowOperation;
    }

    /**
     * 用 StartWorkflowRequest 启动新工作流。
     * 直接把请求包装成 StartWorkflowInput，委托给 StartWorkflowOperation。
     */
    public String startWorkflow(StartWorkflowRequest startWorkflowRequest) {
        return startWorkflowOperation.execute(new StartWorkflowInput(startWorkflowRequest));
    }

    /**
     * 启动新工作流（完整参数版，支持外部存储路径、taskToDomain、内联定义）。
     * 逐个字段组装 StartWorkflowInput，再委托给 StartWorkflowOperation。
     */
    public String startWorkflow(
            String name,
            Integer version,
            String correlationId,
            Integer priority,
            Map<String, Object> input,
            String externalInputPayloadStoragePath,
            Map<String, String> taskToDomain,
            WorkflowDef workflowDef) {
        StartWorkflowInput startWorkflowInput = new StartWorkflowInput();
        startWorkflowInput.setName(name);
        startWorkflowInput.setVersion(version);
        startWorkflowInput.setCorrelationId(correlationId);
        startWorkflowInput.setPriority(priority);
        startWorkflowInput.setWorkflowInput(input);
        startWorkflowInput.setExternalInputPayloadStoragePath(externalInputPayloadStoragePath);
        startWorkflowInput.setTaskToDomain(taskToDomain);
        startWorkflowInput.setWorkflowDefinition(workflowDef);

        return startWorkflowOperation.execute(startWorkflowInput);
    }

    /**
     * 启动新工作流（简化版）：按 name + version 从 MetadataService 查定义，再启动。
     * 若定义不存在，抛 NotFoundException。
     */
    public String startWorkflow(
            String name,
            Integer version,
            String correlationId,
            Integer priority,
            Map<String, Object> input) {
        // 从元数据服务查询工作流定义
        WorkflowDef workflowDef = metadataService.getWorkflowDef(name, version);
        if (workflowDef == null) {
            throw new NotFoundException(
                    "No such workflow found by name: %s, version: %d", name, version);
        }

        StartWorkflowInput startWorkflowInput = new StartWorkflowInput();
        // 用定义中的真实 name/version（version 可能传 null，由 MetadataService 解析为最新）
        startWorkflowInput.setName(workflowDef.getName());
        startWorkflowInput.setVersion(workflowDef.getVersion());
        startWorkflowInput.setCorrelationId(correlationId);
        startWorkflowInput.setPriority(priority);
        startWorkflowInput.setWorkflowInput(input);

        return startWorkflowOperation.execute(startWorkflowInput);
    }

    /** 按 correlationId 列出工作流实例，委托给 ExecutionService */
    public List<Workflow> getWorkflows(
            String name, String correlationId, boolean includeClosed, boolean includeTasks) {
        return executionService.getWorkflowInstances(
                name, correlationId, includeClosed, includeTasks);
    }

    /** 按多个 correlationId 批量列出工作流实例，逐个查询后组装成 Map */
    public Map<String, List<Workflow>> getWorkflows(
            String name, boolean includeClosed, boolean includeTasks, List<String> correlationIds) {
        Map<String, List<Workflow>> workflowMap = new HashMap<>();
        for (String correlationId : correlationIds) {
            List<Workflow> workflows =
                    executionService.getWorkflowInstances(
                            name, correlationId, includeClosed, includeTasks);
            workflowMap.put(correlationId, workflows);
        }
        return workflowMap;
    }

    /** 按 workflowId 获取执行状态，找不到抛 NotFoundException */
    public Workflow getExecutionStatus(String workflowId, boolean includeTasks) {
        Workflow workflow = executionService.getExecutionStatus(workflowId, includeTasks);
        if (workflow == null) {
            throw new NotFoundException("Workflow with id: %s not found.", workflowId);
        }
        return workflow;
    }

    /** 删除工作流（archiveWorkflow=true 时归档而非物理删除） */
    public void deleteWorkflow(String workflowId, boolean archiveWorkflow) {
        executionService.removeWorkflow(workflowId, archiveWorkflow);
    }

    /**
     * 查询运行中的工作流 id 列表。
     * - 若指定了 startTime/endTime：走 workflowExecutor.getWorkflows（按时间范围查）
     * - 否则：解析 version（未指定则取最新），走 getRunningWorkflowIds
     */
    public List<String> getRunningWorkflows(
            String workflowName, Integer version, Long startTime, Long endTime) {
        if (Optional.ofNullable(startTime).orElse(0L) != 0
                && Optional.ofNullable(endTime).orElse(0L) != 0) {
            return workflowExecutor.getWorkflows(workflowName, version, startTime, endTime);
        } else {
            // 未指定 version 时，从 MetadataService 取最新版本
            version =
                    Optional.ofNullable(version)
                            .orElseGet(
                                    () -> {
                                        WorkflowDef workflowDef =
                                                metadataService.getWorkflowDef(workflowName, null);
                                        return workflowDef.getVersion();
                                    });
            return workflowExecutor.getRunningWorkflowIds(workflowName, version);
        }
    }

    /** 手动触发一次 decide，推进工作流 */
    public void decideWorkflow(String workflowId) {
        workflowExecutor.decide(workflowId);
    }

    /** 暂停工作流 */
    public void pauseWorkflow(String workflowId) {
        workflowExecutor.pauseWorkflow(workflowId);
    }

    /** 恢复工作流 */
    public void resumeWorkflow(String workflowId) {
        workflowExecutor.resumeWorkflow(workflowId);
    }

    /** 跳过运行中工作流的指定任务 */
    public void skipTaskFromWorkflow(
            String workflowId, String taskReferenceName, SkipTaskRequest skipTaskRequest) {
        workflowExecutor.skipTaskFromWorkflow(workflowId, taskReferenceName, skipTaskRequest);
    }

    /**
     * 从指定任务开始重跑工作流。
     * 注意：把 workflowId 写入 request 的 reRunFromWorkflowId 字段后再委托。
     */
    public String rerunWorkflow(String workflowId, RerunWorkflowRequest request) {
        request.setReRunFromWorkflowId(workflowId);
        return workflowExecutor.rerun(request);
    }

    /** 重启已完成的工作流（useLatestDefinitions 决定是否用最新定义） */
    public void restartWorkflow(String workflowId, boolean useLatestDefinitions) {
        workflowExecutor.restart(workflowId, useLatestDefinitions);
    }

    /** 重试最后一个失败的任务（resumeSubworkflowTasks 决定是否深入子工作流） */
    public void retryWorkflow(String workflowId, boolean resumeSubworkflowTasks) {
        workflowExecutor.retry(workflowId, resumeSubworkflowTasks);
    }

    /** 重置所有非终态 SIMPLE 任务的回调时间为 0 */
    public void resetWorkflow(String workflowId) {
        workflowExecutor.resetCallbacksForWorkflow(workflowId);
    }

    /** 终止工作流 */
    public void terminateWorkflow(String workflowId, String reason) {
        workflowExecutor.terminateWorkflow(workflowId, reason);
    }

    /** 搜索工作流（摘要版），sort 字符串转成列表后委托给 ExecutionService */
    public SearchResult<WorkflowSummary> searchWorkflows(
            int start, int size, String sort, String freeText, String query) {
        return executionService.search(
                query, freeText, start, size, Utils.convertStringToList(sort));
    }

    /** 搜索工作流（完整版 V2），sort 字符串转成列表后委托 */
    public SearchResult<Workflow> searchWorkflowsV2(
            int start, int size, String sort, String freeText, String query) {
        return executionService.searchV2(
                query, freeText, start, size, Utils.convertStringToList(sort));
    }

    /** 搜索工作流（摘要版，直接传 sort 列表） */
    public SearchResult<WorkflowSummary> searchWorkflows(
            int start, int size, List<String> sort, String freeText, String query) {
        return executionService.search(query, freeText, start, size, sort);
    }

    /** 搜索工作流（完整版 V2，直接传 sort 列表） */
    public SearchResult<Workflow> searchWorkflowsV2(
            int start, int size, List<String> sort, String freeText, String query) {
        return executionService.searchV2(query, freeText, start, size, sort);
    }

    /** 按任务参数搜索工作流（摘要版），sort 字符串转列表 */
    public SearchResult<WorkflowSummary> searchWorkflowsByTasks(
            int start, int size, String sort, String freeText, String query) {
        return executionService.searchWorkflowByTasks(
                query, freeText, start, size, Utils.convertStringToList(sort));
    }

    /** 按任务参数搜索工作流（完整版 V2），sort 字符串转列表 */
    public SearchResult<Workflow> searchWorkflowsByTasksV2(
            int start, int size, String sort, String freeText, String query) {
        return executionService.searchWorkflowByTasksV2(
                query, freeText, start, size, Utils.convertStringToList(sort));
    }

    /** 按任务参数搜索工作流（摘要版，直接传 sort 列表） */
    public SearchResult<WorkflowSummary> searchWorkflowsByTasks(
            int start, int size, List<String> sort, String freeText, String query) {
        return executionService.searchWorkflowByTasks(query, freeText, start, size, sort);
    }

    /** 按任务参数搜索工作流（完整版 V2，直接传 sort 列表） */
    public SearchResult<Workflow> searchWorkflowsByTasksV2(
            int start, int size, List<String> sort, String freeText, String query) {
        return executionService.searchWorkflowByTasksV2(query, freeText, start, size, sort);
    }

    /** 获取外部存储位置，用于读写工作流 input/output payload */
    public ExternalStorageLocation getExternalStorageLocation(
            String path, String operation, String type) {
        return executionService.getExternalStorageLocation(path, operation, type);
    }
}