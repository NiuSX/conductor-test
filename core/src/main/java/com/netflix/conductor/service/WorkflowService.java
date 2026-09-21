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

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import org.springframework.validation.annotation.Validated;

import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.SkipTaskRequest;
import com.netflix.conductor.common.metadata.workflow.StartWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.ExternalStorageLocation;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;

/**
 * 工作流服务接口：定义工作流生命周期管理的对外 API 契约。
 *
 * 它是 Conductor 的"门面"层接口，上层（REST API / gRPC）和下层（WorkflowExecutor）
 * 都通过它交互。所有方法都带 javax.validation 注解，由 Spring 在入口处做参数校验。
 *
 * 职责范围：
 * - 启动工作流（多种重载）
 * - 查询工作流（按 id、correlationId、运行中、搜索）
 * - 生命周期操作（暂停/恢复/跳过/重跑/重启/重试/终止/删除）
 * - 外部存储位置获取
 */
@Validated
public interface WorkflowService {

    /**
     * 用 StartWorkflowRequest 启动新工作流（支持指定 task domain）。
     *
     * @param startWorkflowRequest 启动请求
     * @return 工作流实例 id，可用于追踪
     */
    String startWorkflow(
            @NotNull(message = "StartWorkflowRequest cannot be null") @Valid
            StartWorkflowRequest startWorkflowRequest);

    /**
     * 启动新工作流（简化版）。返回工作流实例 id。
     *
     * @param name 工作流名称
     * @param version 工作流版本
     * @param correlationId 关联 id
     * @param priority 优先级（0~99）
     * @param input 输入参数
     * @return 工作流实例 id
     */
    String startWorkflow(
            @NotEmpty(message = "Workflow name cannot be null or empty") String name,
            Integer version,
            String correlationId,
            @Min(value = 0, message = "0 is the minimum priority value")
            @Max(value = 99, message = "99 is the maximum priority value")
            Integer priority,
            Map<String, Object> input);

    /**
     * 启动新工作流（完整参数版）。额外支持外部输入存储路径、taskToDomain、内联工作流定义。
     *
     * @param name 工作流名称
     * @param version 工作流版本
     * @param correlationId 关联 id
     * @param priority 优先级
     * @param input 输入参数
     * @param externalInputPayloadStoragePath 输入被外部化存储的路径
     * @param taskToDomain 任务到 domain 的映射
     * @param workflowDef 内联工作流定义
     * @return 工作流实例 id
     */
    String startWorkflow(
            String name,
            Integer version,
            String correlationId,
            Integer priority,
            Map<String, Object> input,
            String externalInputPayloadStoragePath,
            Map<String, String> taskToDomain,
            WorkflowDef workflowDef);

    /**
     * 按 correlationId 列出工作流。
     *
     * @param name 工作流名称
     * @param correlationId 关联 id
     * @param includeClosed 是否包含已结束的工作流
     * @param includeTasks 是否包含任务详情
     * @return 工作流列表
     */
    List<Workflow> getWorkflows(
            @NotEmpty(message = "Workflow name cannot be null or empty") String name,
            String correlationId,
            boolean includeClosed,
            boolean includeTasks);

    /**
     * 按多个 correlationId 批量列出工作流。
     *
     * @param name 工作流名称
     * @param includeClosed 是否包含已结束的工作流
     * @param includeTasks 是否包含任务详情
     * @param correlationIds 关联 id 列表
     * @return correlationId → 工作流列表 的映射
     */
    Map<String, List<Workflow>> getWorkflows(
            @NotEmpty(message = "Workflow name cannot be null or empty") String name,
            boolean includeClosed,
            boolean includeTasks,
            List<String> correlationIds);

    /**
     * 按 workflowId 获取工作流执行状态。
     *
     * @param workflowId 工作流 id
     * @param includeTasks 是否包含任务详情
     * @return 工作流实例
     */
    Workflow getExecutionStatus(
            @NotEmpty(message = "WorkflowId cannot be null or empty.") String workflowId,
            boolean includeTasks);

    /**
     * 从系统中删除工作流。
     *
     * @param workflowId 工作流 id
     * @param archiveWorkflow true 表示归档（保留）而非物理删除
     */
    void deleteWorkflow(
            @NotEmpty(message = "WorkflowId cannot be null or empty.") String workflowId,
            boolean archiveWorkflow);

    /**
     * 查询运行中的工作流 id 列表。
     *
     * @param workflowName 工作流名称
     * @param version 版本
     * @param startTime 起始时间
     * @param endTime 结束时间
     * @return 工作流 id 列表
     */
    List<String> getRunningWorkflows(
            @NotEmpty(message = "Workflow name cannot be null or empty.") String workflowName,
            Integer version,
            Long startTime,
            Long endTime);

    /**
     * 触发工作流的决策任务（手动推进一次 decide）。
     *
     * @param workflowId 工作流 id
     */
    void decideWorkflow(
            @NotEmpty(message = "WorkflowId cannot be null or empty.") String workflowId);

    /**
     * 暂停工作流。
     *
     * @param workflowId 工作流 id
     */
    void pauseWorkflow(
            @NotEmpty(message = "WorkflowId cannot be null or empty.") String workflowId);

    /**
     * 恢复工作流。
     *
     * @param workflowId 工作流 id
     */
    void resumeWorkflow(
            @NotEmpty(message = "WorkflowId cannot be null or empty.") String workflowId);

    /**
     * 跳过运行中工作流的某个任务。
     *
     * @param workflowId 工作流 id
     * @param taskReferenceName 任务引用名
     * @param skipTaskRequest 跳过请求（可携带输入/输出）
     */
    void skipTaskFromWorkflow(
            @NotEmpty(message = "WorkflowId name cannot be null or empty.") String workflowId,
            @NotEmpty(message = "TaskReferenceName cannot be null or empty.")
            String taskReferenceName,
            SkipTaskRequest skipTaskRequest);

    /**
     * 从指定任务开始重跑工作流。
     *
     * @param workflowId 工作流 id
     * @param request 重跑请求
     * @return 重跑后的工作流 id
     */
    String rerunWorkflow(
            @NotEmpty(message = "WorkflowId cannot be null or empty.") String workflowId,
            @NotNull(message = "RerunWorkflowRequest cannot be null.")
            RerunWorkflowRequest request);

    /**
     * 重启一个已完成的工作流。
     *
     * @param workflowId 工作流 id
     * @param useLatestDefinitions 是否使用最新定义
     */
    void restartWorkflow(
            @NotEmpty(message = "WorkflowId cannot be null or empty.") String workflowId,
            boolean useLatestDefinitions);

    /**
     * 重试最后一个失败的任务。
     *
     * @param workflowId 工作流 id
     * @param resumeSubworkflowTasks 是否深入子工作流重试
     */
    void retryWorkflow(
            @NotEmpty(message = "WorkflowId cannot be null or empty.") String workflowId,
            boolean resumeSubworkflowTasks);

    /**
     * 重置所有非终态 SIMPLE 任务的回调时间为 0（让它们立即被重新调度）。
     *
     * @param workflowId 工作流 id
     */
    void resetWorkflow(
            @NotEmpty(message = "WorkflowId cannot be null or empty.") String workflowId);

    /**
     * 终止工作流执行。
     *
     * @param workflowId 工作流 id
     * @param reason 终止原因
     */
    void terminateWorkflow(
            @NotEmpty(message = "WorkflowId cannot be null or empty.") String workflowId,
            String reason);

    /**
     * 按 payload 和参数搜索工作流（返回摘要）。sort 支持 ASC/DESC，如 sort=name 或 sort=workflowId:DESC。
     *
     * @param start 分页起始
     * @param size 返回条数（上限 5000）
     * @param sort 排序
     * @param freeText 全文搜索
     * @param query 结构化查询
     * @return 搜索结果
     */
    SearchResult<WorkflowSummary> searchWorkflows(
            int start,
            @Max(
                    value = 5_000,
                    message =
                            "Cannot return more than {value} workflows. Please use pagination.")
            int size,
            String sort,
            String freeText,
            String query);

    /**
     * 搜索工作流（返回完整 Workflow，V2 版本）。
     * 参数含义同 searchWorkflows。
     */
    SearchResult<Workflow> searchWorkflowsV2(
            int start,
            @Max(
                    value = 5_000,
                    message =
                            "Cannot return more than {value} workflows. Please use pagination.")
            int size,
            String sort,
            String freeText,
            String query);

    /**
     * 搜索工作流（摘要版，支持多字段排序）。
     *
     * @param sort 排序字段列表，用 "|" 分隔
     */
    SearchResult<WorkflowSummary> searchWorkflows(
            int start,
            @Max(
                    value = 5_000,
                    message =
                            "Cannot return more than {value} workflows. Please use pagination.")
            int size,
            List<String> sort,
            String freeText,
            String query);

    /**
     * 搜索工作流（完整版，支持多字段排序）。
     */
    SearchResult<Workflow> searchWorkflowsV2(
            int start,
            @Max(
                    value = 5_000,
                    message =
                            "Cannot return more than {value} workflows. Please use pagination.")
            int size,
            List<String> sort,
            String freeText,
            String query);

    /**
     * 按任务参数搜索工作流（摘要版）。
     */
    SearchResult<WorkflowSummary> searchWorkflowsByTasks(
            int start, int size, String sort, String freeText, String query);

    /**
     * 按任务参数搜索工作流（完整版，V2）。
     */
    SearchResult<Workflow> searchWorkflowsByTasksV2(
            int start, int size, String sort, String freeText, String query);

    /**
     * 按任务参数搜索工作流（摘要版，支持多字段排序）。
     */
    SearchResult<WorkflowSummary> searchWorkflowsByTasks(
            int start, int size, List<String> sort, String freeText, String query);

    /**
     * 按任务参数搜索工作流（完整版，支持多字段排序，V2）。
     */
    SearchResult<Workflow> searchWorkflowsByTasksV2(
            int start, int size, List<String> sort, String freeText, String query);

    /**
     * 获取外部存储位置，用于读写工作流输入/输出 payload。
     *
     * @param path 存储路径
     * @param operation 操作（read / write）
     * @param payloadType payload 类型（input / output）
     * @return 包含 uri 和路径的 ExternalStorageLocation
     */
    ExternalStorageLocation getExternalStorageLocation(
            String path, String operation, String payloadType);
}