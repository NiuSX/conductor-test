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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.netflix.conductor.annotations.Audit;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.run.ExternalStorageLocation;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;

/**
 * TaskService 的实现类：面向 Worker 的任务交互门面。
 *
 * 它是薄封装 + 委托：
 * - 大部分方法委托给 ExecutionService
 * - 队列详情查询直接委托 QueueDAO
 *
 * 唯一有实质逻辑的是 ackTaskReceived：当 ack 失败时，
 * 把任务标记为 FAILED，让 decide 重新评估工作流，避免工作流卡住。
 *
 * @Audit 记录审计日志
 * @Trace 记录调用链追踪
 */
@Audit
@Trace
@Service
public class TaskServiceImpl implements TaskService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskServiceImpl.class);
    private final ExecutionService executionService;
    private final QueueDAO queueDAO;

    public TaskServiceImpl(ExecutionService executionService, QueueDAO queueDAO) {
        this.executionService = executionService;
        this.queueDAO = queueDAO;
    }

    // ==================== 任务拉取 ====================

    /**
     * 拉取单个任务。
     * 委托给 executionService.getLastPollTask（内部用 100ms 短轮询）。
     * 记录 poll 计数指标。
     */
    public Task poll(String taskType, String workerId, String domain) {
        LOGGER.debug("Task being polled: /tasks/poll/{}?{}&{}", taskType, workerId, domain);
        Task task = executionService.getLastPollTask(taskType, workerId, domain);
        if (task != null) {
            LOGGER.debug(
                    "The Task {} being returned for /tasks/poll/{}?{}&{}",
                    task,
                    taskType,
                    workerId,
                    domain);
        }
        Monitors.recordTaskPollCount(taskType, domain, 1);
        return task;
    }

    /**
     * 批量拉取任务。
     * 委托给 executionService.poll，并记录拉取到的任务数指标。
     */
    public List<Task> batchPoll(
            String taskType, String workerId, String domain, Integer count, Integer timeout) {
        List<Task> polledTasks = executionService.poll(taskType, workerId, domain, count, timeout);
        LOGGER.debug(
                "The Tasks {} being returned for /tasks/poll/{}?{}&{}",
                polledTasks.stream().map(Task::getTaskId).collect(Collectors.toList()),
                taskType,
                workerId,
                domain);
        Monitors.recordTaskPollCount(taskType, domain, polledTasks.size());
        return polledTasks;
    }

    // ==================== 任务查询 ====================

    /** 分页查询进行中的任务 */
    public List<Task> getTasks(String taskType, String startKey, Integer count) {
        return executionService.getTasks(taskType, startKey, count);
    }

    /**
     * 查询指定工作流中某引用名的待处理任务。
     * 注意：这里把参数顺序调整为 (workflowId, taskReferenceName) 后再传给 ExecutionService
     * （ExecutionService 的签名是 (taskReferenceName, workflowId)）。
     */
    public Task getPendingTaskForWorkflow(String workflowId, String taskReferenceName) {
        return executionService.getPendingTaskForWorkflow(taskReferenceName, workflowId);
    }

    /** 按 taskId 查询任务 */
    public Task getTask(String taskId) {
        return executionService.getTask(taskId);
    }

    // ==================== 任务更新 ====================

    /**
     * 更新任务结果（Worker 上报）。
     * 委托给 executionService.updateTask，返回被更新任务的 id。
     */
    public String updateTask(TaskResult taskResult) {
        LOGGER.debug(
                "Update Task: {} with callback time: {}",
                taskResult,
                taskResult.getCallbackAfterSeconds());
        executionService.updateTask(taskResult);
        LOGGER.debug(
                "Task: {} updated successfully with callback time: {}",
                taskResult,
                taskResult.getCallbackAfterSeconds());
        return taskResult.getTaskId();
    }

    // ==================== 任务确认（含容错）====================

    /** ack 任务（带 workerId），返回字符串形式的确认结果 */
    public String ackTaskReceived(String taskId, String workerId) {
        LOGGER.debug("Ack received for task: {} from worker: {}", taskId, workerId);
        return String.valueOf(ackTaskReceived(taskId));
    }

    /**
     * ack 任务（核心逻辑）。
     *
     * 正常情况：委托 executionService.ackTaskReceived。
     * 异常情况（关键容错）：
     * - 记录错误日志
     * - 把任务标记为 FAILED（failTask）
     * - 返回 false
     *
     * 目的：ack 失败时不让工作流卡住，而是让 decide 重新评估工作流。
     * 这是"至少一次投递 + 幂等"设计的一部分——宁可失败重试，也不静默卡住。
     */
    public boolean ackTaskReceived(String taskId) {
        LOGGER.debug("Ack received for task: {}", taskId);
        AtomicBoolean ackResult = new AtomicBoolean(false);
        try {
            ackResult.set(executionService.ackTaskReceived(taskId));
        } catch (Exception e) {
            // ack 失败时把任务标记为 FAILED，让 decide 重新评估工作流，避免工作流卡住
            String errorMsg = String.format("Error when trying to ack task %s", taskId);
            LOGGER.error(errorMsg, e);
            Task task = executionService.getTask(taskId);
            Monitors.recordAckTaskError(task.getTaskType());
            failTask(task, errorMsg);
            ackResult.set(false);
        }
        return ackResult.get();
    }

    /**
     * 把任务标记为 FAILED。
     * 若更新任务也失败，则终止整个工作流（兜底，避免工作流永久卡住）。
     */
    private void failTask(Task task, String errorMsg) {
        try {
            TaskResult taskResult = new TaskResult();
            taskResult.setStatus(TaskResult.Status.FAILED);
            taskResult.setTaskId(task.getTaskId());
            taskResult.setWorkflowInstanceId(task.getWorkflowInstanceId());
            taskResult.setReasonForIncompletion(errorMsg);
            executionService.updateTask(taskResult);
        } catch (Exception e) {
            // 连标记失败都做不到，只能终止工作流兜底
            LOGGER.error(
                    "Unable to fail task: {} in workflow: {}",
                    task.getTaskId(),
                    task.getWorkflowInstanceId(),
                    e);
            executionService.terminateWorkflow(
                    task.getWorkflowInstanceId(), "Failed to ack task: " + task.getTaskId());
        }
    }

    // ==================== 任务日志 ====================

    /** 记录任务执行日志 */
    public void log(String taskId, String log) {
        executionService.log(taskId, log);
    }

    /** 获取任务执行日志 */
    public List<TaskExecLog> getTaskLogs(String taskId) {
        return executionService.getTaskLogs(taskId);
    }

    // ==================== 队列管理 ====================

    /** 从队列移除任务（带 taskType 参数，但实际只用 taskId） */
    public void removeTaskFromQueue(String taskType, String taskId) {
        executionService.removeTaskFromQueue(taskId);
    }

    /** 从队列移除任务 */
    public void removeTaskFromQueue(String taskId) {
        executionService.removeTaskFromQueue(taskId);
    }

    /** 批量查询任务类型队列大小 */
    public Map<String, Integer> getTaskQueueSizes(List<String> taskTypes) {
        return executionService.getTaskQueueSizes(taskTypes);
    }

    /**
     * 查询指定任务类型的队列大小。
     * 这里需要先根据 taskType + domain + isolationGroupId + executionNamespace
     * 组装出实际的队列名，再查询。
     */
    @Override
    public Integer getTaskQueueSize(
            String taskType, String domain, String isolationGroupId, String executionNamespace) {
        String queueName =
                QueueUtils.getQueueName(
                        taskType,
                        StringUtils.trimToNull(domain),
                        StringUtils.trimToNull(isolationGroupId),
                        StringUtils.trimToNull(executionNamespace));

        return executionService.getTaskQueueSize(queueName);
    }

    /** 查询每个队列的详细信息（verbose），直接委托 QueueDAO */
    public Map<String, Map<String, Map<String, Long>>> allVerbose() {
        return queueDAO.queuesDetailVerbose();
    }

    /**
     * 查询每个队列的详情（简版）。
     * 按队列名排序后收集为 LinkedHashMap，保证返回顺序稳定。
     */
    public Map<String, Long> getAllQueueDetails() {
        return queueDAO.queuesDetail().entrySet().stream()
                .sorted(Entry.comparingByKey())
                .collect(
                        Collectors.toMap(
                                Entry::getKey,
                                Entry::getValue,
                                (v1, v2) -> v1,
                                LinkedHashMap::new));
    }

    // ==================== Poll 数据 ====================

    /** 查询指定任务类型的 poll 数据 */
    public List<PollData> getPollData(String taskType) {
        return executionService.getPollData(taskType);
    }

    /** 查询所有任务类型的 poll 数据 */
    public List<PollData> getAllPollData() {
        return executionService.getAllPollData();
    }

    // ==================== 重新入队 ====================

    /** 重新入队待处理任务，返回入队任务数（字符串形式） */
    public String requeuePendingTask(String taskType) {
        return String.valueOf(executionService.requeuePendingTasks(taskType));
    }

    // ==================== 搜索 ====================

    /** 搜索任务摘要 */
    public SearchResult<TaskSummary> search(
            int start, int size, String sort, String freeText, String query) {
        return executionService.getSearchTasks(query, freeText, start, size, sort);
    }

    /** 搜索任务完整版 V2 */
    public SearchResult<Task> searchV2(
            int start, int size, String sort, String freeText, String query) {
        return executionService.getSearchTasksV2(query, freeText, start, size, sort);
    }

    // ==================== 外部存储 ====================

    /** 获取任务输出 payload 的外部存储位置 */
    public ExternalStorageLocation getExternalStorageLocation(
            String path, String operation, String type) {
        return executionService.getExternalStorageLocation(path, operation, type);
    }
}