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
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import org.springframework.validation.annotation.Validated;

import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.ExternalStorageLocation;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.TaskSummary;

/**
 * 任务服务接口：定义 Worker 与任务交互的对外 API 契约。
 *
 * 它是"任务执行侧"的门面接口，主要面向 Worker：
 * - poll / batchPoll：Worker 拉取任务
 * - updateTask：Worker 上报任务结果
 * - ackTaskReceived：确认收到任务
 * - log / getTaskLogs：任务日志
 * - 队列管理、poll 数据、搜索
 *
 * 与 WorkflowService 的分工：
 * - WorkflowService：管理工作流实例（启动、暂停、重试等）
 * - TaskService（本接口）：管理任务的执行交互
 *
 * 实现上，它通常委托给 ExecutionService。
 */
@Validated
public interface TaskService {

    /**
     * 拉取一个指定类型的任务（单条）。
     *
     * @param taskType 任务名称
     * @param workerId Worker id
     * @param domain 目标 domain
     * @return 拉取到的任务
     */
    Task poll(
            @NotEmpty(message = "TaskType cannot be null or empty.") String taskType,
            String workerId,
            String domain);

    /**
     * 批量拉取指定类型的任务。
     *
     * @param taskType 任务名称
     * @param workerId Worker id
     * @param domain 目标 domain
     * @param count 期望拉取数量
     * @param timeout 长轮询超时（毫秒）
     * @return 任务列表
     */
    List<Task> batchPoll(
            @NotEmpty(message = "TaskType cannot be null or empty.") String taskType,
            String workerId,
            String domain,
            Integer count,
            Integer timeout);

    /**
     * 分页查询进行中的任务。
     *
     * @param taskType 任务名称
     * @param startKey 分页起始 key
     * @param count 返回条数
     * @return 任务列表
     */
    List<Task> getTasks(
            @NotEmpty(message = "TaskType cannot be null or empty.") String taskType,
            String startKey,
            Integer count);

    /**
     * 查询指定工作流中某引用名的待处理任务。
     *
     * @param workflowId 工作流 id
     * @param taskReferenceName 任务引用名
     * @return 任务实例
     */
    Task getPendingTaskForWorkflow(
            @NotEmpty(message = "WorkflowId cannot be null or empty.") String workflowId,
            @NotEmpty(message = "TaskReferenceName cannot be null or empty.")
            String taskReferenceName);

    /**
     * 更新任务结果（Worker 上报执行结果）。
     *
     * @param taskResult 任务结果
     * @return 被更新任务的 id
     */
    String updateTask(
            @NotNull(message = "TaskResult cannot be null or empty.") @Valid TaskResult taskResult);

    /**
     * 确认任务已收到（带 workerId）。
     *
     * @param taskId 任务 id
     * @param workerId Worker id
     * @return 确认结果的字符串表示
     */
    String ackTaskReceived(
            @NotEmpty(message = "TaskId cannot be null or empty.") String taskId, String workerId);

    /**
     * 确认任务已收到。
     *
     * @param taskId 任务 id
     * @return 是否成功确认
     */
    boolean ackTaskReceived(@NotEmpty(message = "TaskId cannot be null or empty.") String taskId);

    /**
     * 记录任务执行日志。
     *
     * @param taskId 任务 id
     * @param log 日志内容
     */
    void log(@NotEmpty(message = "TaskId cannot be null or empty.") String taskId, String log);

    /**
     * 获取任务执行日志。
     *
     * @param taskId 任务 id
     * @return 日志列表
     */
    List<TaskExecLog> getTaskLogs(
            @NotEmpty(message = "TaskId cannot be null or empty.") String taskId);

    /**
     * 按 taskId 查询任务。
     *
     * @param taskId 任务 id
     * @return 任务实例
     */
    Task getTask(@NotEmpty(message = "TaskId cannot be null or empty.") String taskId);

    /**
     * 从任务类型队列中移除任务（带 taskType）。
     *
     * @param taskType 任务名称
     * @param taskId 任务 id
     */
    void removeTaskFromQueue(
            @NotEmpty(message = "TaskType cannot be null or empty.") String taskType,
            @NotEmpty(message = "TaskId cannot be null or empty.") String taskId);

    /**
     * 从任务类型队列中移除任务（仅 taskId）。
     *
     * @param taskId 任务 id
     */
    void removeTaskFromQueue(@NotEmpty(message = "TaskId cannot be null or empty.") String taskId);

    /**
     * 批量查询任务类型队列大小。
     *
     * @param taskTypes 任务类型列表
     * @return 任务类型 → 队列大小
     */
    Map<String, Integer> getTaskQueueSizes(List<String> taskTypes);

    /**
     * 查询指定任务类型的队列大小（可带 domain、isolationGroupId、executionNamespace）。
     */
    Integer getTaskQueueSize(
            String taskType, String domain, String isolationGroupId, String executionNamespace);

    /**
     * 查询每个队列的详细信息（verbose）。
     *
     * @return 队列详情
     */
    Map<String, Map<String, Map<String, Long>>> allVerbose();

    /**
     * 查询每个队列的详情（简版）。
     *
     * @return 队列详情
     */
    Map<String, Long> getAllQueueDetails();

    /**
     * 查询指定任务类型的最后 poll 数据。
     *
     * @param taskType 任务名称
     * @return poll 数据列表
     */
    List<PollData> getPollData(
            @NotEmpty(message = "TaskType cannot be null or empty.") String taskType);

    /**
     * 查询所有任务类型的最后 poll 数据。
     *
     * @return poll 数据列表
     */
    List<PollData> getAllPollData();

    /**
     * 重新入队待处理任务。
     *
     * @param taskType 任务名称
     * @return 重新入队的任务数（字符串形式）
     */
    String requeuePendingTask(
            @NotEmpty(message = "TaskType cannot be null or empty.") String taskType);

    /**
     * 按 payload 和参数搜索任务（摘要版）。sort 支持 ASC/DESC，如 sort=name 或 sort=workflowId。
     *
     * @param start 分页起始
     * @param size 返回条数
     * @param sort 排序
     * @param freeText 全文搜索
     * @param query 结构化查询
     * @return 搜索结果
     */
    SearchResult<TaskSummary> search(
            int start, int size, String sort, String freeText, String query);

    /**
     * 按 payload 和参数搜索任务（完整版 V2）。
     * 参数含义同 search。
     */
    SearchResult<Task> searchV2(int start, int size, String sort, String freeText, String query);

    /**
     * 获取任务输出 payload 的外部存储位置。
     *
     * @param path 路径
     * @param operation 操作（read / write）
     * @param payloadType payload 类型（input / output）
     * @return 外部存储位置
     */
    ExternalStorageLocation getExternalStorageLocation(
            String path, String operation, String payloadType);
}