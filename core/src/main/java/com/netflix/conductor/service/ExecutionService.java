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

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.*;
import com.netflix.conductor.common.run.*;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;
import com.netflix.conductor.common.utils.ExternalPayloadStorage.Operation;
import com.netflix.conductor.common.utils.ExternalPayloadStorage.PayloadType;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.SystemTaskRegistry;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.core.utils.Utils;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.TaskModel;

/**
 * 执行服务：负责与"任务执行"相关的所有操作，尤其是 Worker 的任务拉取（poll）。
 *
 * 它是 Conductor 的"执行侧门面"，主要职责：
 * - 任务 poll（Worker 从队列取任务）
 * - 任务 ack（确认收到）
 * - 队列大小查询、任务重新入队
 * - 工作流实例/任务查询、搜索
 * - 任务日志、事件执行记录
 * - 外部存储位置获取
 *
 * 它与 WorkflowService 的分工：
 * - WorkflowService：管理工作流实例的生命周期（启动、暂停、重试等）
 * - ExecutionService（本类）：管理任务的执行交互（poll、ack、队列操作）
 */
@Trace
@Service
public class ExecutionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionService.class);

    private final WorkflowExecutor workflowExecutor;
    private final ExecutionDAOFacade executionDAOFacade;
    private final QueueDAO queueDAO;
    private final ExternalPayloadStorage externalPayloadStorage;
    private final SystemTaskRegistry systemTaskRegistry;

    /** 任务消息被 postpone（推迟）的秒数，用于超出并发/频率限制时 */
    private final long queueTaskMessagePostponeSecs;

    /** 长轮询的最大超时时间（5 秒） */
    private static final int MAX_POLL_TIMEOUT_MS = 5000;
    private static final int POLL_COUNT_ONE = 1;
    private static final int POLLING_TIMEOUT_IN_MS = 100;

    public ExecutionService(
            WorkflowExecutor workflowExecutor,
            ExecutionDAOFacade executionDAOFacade,
            QueueDAO queueDAO,
            ConductorProperties properties,
            ExternalPayloadStorage externalPayloadStorage,
            SystemTaskRegistry systemTaskRegistry) {
        this.workflowExecutor = workflowExecutor;
        this.executionDAOFacade = executionDAOFacade;
        this.queueDAO = queueDAO;
        this.externalPayloadStorage = externalPayloadStorage;

        this.queueTaskMessagePostponeSecs =
                properties.getTaskExecutionPostponeDuration().getSeconds();
        this.systemTaskRegistry = systemTaskRegistry;
    }

    // ==================== 任务 poll（Worker 拉取任务）====================

    /** poll 单个任务（无 domain） */
    public Task poll(String taskType, String workerId) {
        return poll(taskType, workerId, null);
    }

    /** poll 单个任务（指定 domain）：内部调用批量 poll，取第一个 */
    public Task poll(String taskType, String workerId, String domain) {

        List<Task> tasks = poll(taskType, workerId, domain, 1, 100);
        if (tasks.isEmpty()) {
            return null;
        }
        return tasks.get(0);
    }

    /** poll 多个任务（无 domain） */
    public List<Task> poll(String taskType, String workerId, int count, int timeoutInMilliSecond) {
        return poll(taskType, workerId, null, count, timeoutInMilliSecond);
    }

    /**
     * poll 任务的核心实现：
     * 1. 从队列 pop 出 taskId
     * 2. 对每个 taskId：加载任务 → 校验状态 → 检查并发/频率限制 → 置为 IN_PROGRESS → 返回给 Worker
     * 3. 超出限制的任务 postpone（推迟），让其他 Worker 有机会处理
     *
     * @param taskType 任务类型
     * @param workerId Worker id
     * @param domain 目标 domain（可为 null）
     * @param count 期望拉取的任务数
     * @param timeoutInMilliSecond 长轮询超时（上限 5 秒）
     * @return 拉取到的任务列表
     */
    public List<Task> poll(
            String taskType, String workerId, String domain, int count, int timeoutInMilliSecond) {
        // 长轮询超时上限校验
        if (timeoutInMilliSecond > MAX_POLL_TIMEOUT_MS) {
            throw new IllegalArgumentException(
                    "Long Poll Timeout value cannot be more than 5 seconds");
        }
        String queueName = QueueUtils.getQueueName(taskType, domain, null, null);

        List<String> taskIds = new LinkedList<>();
        List<Task> tasks = new LinkedList<>();
        try {
            // 从队列 pop 出 taskId
            taskIds = queueDAO.pop(queueName, count, timeoutInMilliSecond);
        } catch (Exception e) {
            LOGGER.error(
                    "Error polling for task: {} from worker: {} in domain: {}, count: {}",
                    taskType,
                    workerId,
                    domain,
                    count,
                    e);
            Monitors.error(this.getClass().getCanonicalName(), "taskPoll");
            Monitors.recordTaskPollError(taskType, domain, e.getClass().getSimpleName());
        }

        for (String taskId : taskIds) {
            try {
                TaskModel taskModel = executionDAOFacade.getTaskModel(taskId);
                // 任务不存在或已终态：从队列移除，跳过
                if (taskModel == null || taskModel.getStatus().isTerminal()) {
                    queueDAO.remove(queueName, taskId);
                    LOGGER.debug("Removed task: {} from the queue: {}", taskId, queueName);
                    continue;
                }

                // 超过 in-progress 并发限制：postpone，等下次再 poll
                if (executionDAOFacade.exceedsInProgressLimit(taskModel)) {
                    queueDAO.postpone(
                            queueName,
                            taskId,
                            taskModel.getWorkflowPriority(),
                            queueTaskMessagePostponeSecs);
                    LOGGER.debug(
                            "Postponed task: {} in queue: {} by {} seconds",
                            taskId,
                            queueName,
                            queueTaskMessagePostponeSecs);
                    continue;
                }
                TaskDef taskDef =
                        taskModel.getTaskDefinition().isPresent()
                                ? taskModel.getTaskDefinition().get()
                                : null;
                // 超过频率限制：postpone，等下次再 poll
                if (taskModel.getRateLimitPerFrequency() > 0
                        && executionDAOFacade.exceedsRateLimitPerFrequency(taskModel, taskDef)) {
                    queueDAO.postpone(
                            queueName,
                            taskId,
                            taskModel.getWorkflowPriority(),
                            queueTaskMessagePostponeSecs);
                    LOGGER.debug(
                            "RateLimit Execution limited for {}:{}, limit:{}",
                            taskId,
                            taskModel.getTaskDefName(),
                            taskModel.getRateLimitPerFrequency());
                    continue;
                }

                // 任务置为 IN_PROGRESS，记录开始时间
                taskModel.setStatus(TaskModel.Status.IN_PROGRESS);
                if (taskModel.getStartTime() == 0) {
                    taskModel.setStartTime(System.currentTimeMillis());
                    Monitors.recordQueueWaitTime(
                            taskModel.getTaskDefName(), taskModel.getQueueWaitTime());
                }
                // 交给 Worker 时重置 callbackAfterSeconds
                taskModel.setCallbackAfterSeconds(0);
                taskModel.setWorkerId(workerId);
                taskModel.incrementPollCount();
                executionDAOFacade.updateTask(taskModel);
                tasks.add(taskModel.toTask());
            } catch (Exception e) {
                // DB 操作失败：把任务放回队列并延迟，避免任务丢失
                LOGGER.warn(
                        "DB operation failed for task: {}, postponing task in queue", taskId, e);
                Monitors.recordTaskPollError(taskType, domain, e.getClass().getSimpleName());
                queueDAO.postpone(queueName, taskId, 0, queueTaskMessagePostponeSecs);
            }
        }
        // 更新该任务类型/domain 的最后 poll 时间（用于判断 domain 是否活跃）
        executionDAOFacade.updateTaskLastPoll(taskType, domain, workerId);
        Monitors.recordTaskPoll(queueName);
        tasks.forEach(this::ackTaskReceived);
        return tasks;
    }

    /** 获取最后一个 poll 的任务（带 100ms 短轮询） */
    public Task getLastPollTask(String taskType, String workerId, String domain) {
        List<Task> tasks = poll(taskType, workerId, domain, POLL_COUNT_ONE, POLLING_TIMEOUT_IN_MS);
        if (tasks.isEmpty()) {
            LOGGER.debug(
                    "No Task available for the poll: /tasks/poll/{}?{}&{}",
                    taskType,
                    workerId,
                    domain);
            return null;
        }
        Task task = tasks.get(0);
        ackTaskReceived(task);
        LOGGER.debug(
                "The Task {} being returned for /tasks/poll/{}?{}&{}",
                task,
                taskType,
                workerId,
                domain);
        return task;
    }

    // ==================== Poll 数据查询 ====================

    /** 查询指定任务类型的 poll 数据（各 domain/worker 的活跃情况） */
    public List<PollData> getPollData(String taskType) {
        return executionDAOFacade.getTaskPollData(taskType);
    }

    /** 查询所有 poll 数据（DAO 不支持时回退到按队列名逐个查询） */
    public List<PollData> getAllPollData() {
        try {
            return executionDAOFacade.getAllPollData();
        } catch (UnsupportedOperationException uoe) {
            List<PollData> allPollData = new ArrayList<>();
            Map<String, Long> queueSizes = queueDAO.queuesDetail();
            queueSizes
                    .keySet()
                    .forEach(
                            queueName -> {
                                try {
                                    // 只处理不带 domain 分隔符的队列名
                                    if (!queueName.contains(QueueUtils.DOMAIN_SEPARATOR)) {
                                        allPollData.addAll(
                                                getPollData(
                                                        QueueUtils.getQueueNameWithoutDomain(
                                                                queueName)));
                                    }
                                } catch (Exception e) {
                                    LOGGER.error("Unable to fetch all poll data!", e);
                                }
                            });
            return allPollData;
        }
    }

    // ==================== 工作流/任务操作委托 ====================

    /** 终止工作流，委托给 WorkflowExecutor */
    public void terminateWorkflow(String workflowId, String reason) {
        workflowExecutor.terminateWorkflow(workflowId, reason);
    }

    /** 更新任务结果（Worker 上报），委托给 WorkflowExecutor */
    public void updateTask(TaskResult taskResult) {
        workflowExecutor.updateTask(taskResult);
    }

    /** 按任务类型分页查询任务 */
    public List<Task> getTasks(String taskType, String startKey, int count) {
        return executionDAOFacade.getTasksByName(taskType, startKey, count);
    }

    /** 按 taskId 查询任务 */
    public Task getTask(String taskId) {
        return executionDAOFacade.getTask(taskId);
    }

    /** 查询工作流中指定引用名的待处理任务（同一引用名同时只有一个在运行） */
    public Task getPendingTaskForWorkflow(String taskReferenceName, String workflowId) {
        return executionDAOFacade.getTasksForWorkflow(workflowId).stream()
                .filter(task -> !task.getStatus().isTerminal())
                .filter(task -> task.getReferenceTaskName().equals(taskReferenceName))
                .findFirst()
                .orElse(null);
    }

    /**
     * 确认任务已收到（从 un-acked 队列移除）。
     *
     * @param taskId 任务 id
     * @return 是否成功移除
     */
    public boolean ackTaskReceived(String taskId) {
        return Optional.ofNullable(getTask(taskId)).map(this::ackTaskReceived).orElse(false);
    }

    /** 确认任务已收到（从队列 ack） */
    public boolean ackTaskReceived(Task task) {
        return queueDAO.ack(QueueUtils.getQueueName(task), task.getTaskId());
    }

    /** 批量查询任务队列大小 */
    public Map<String, Integer> getTaskQueueSizes(List<String> taskDefNames) {
        Map<String, Integer> sizes = new HashMap<>();
        for (String taskDefName : taskDefNames) {
            sizes.put(taskDefName, getTaskQueueSize(taskDefName));
        }
        return sizes;
    }

    /** 查询指定队列大小 */
    public Integer getTaskQueueSize(String queueName) {
        return queueDAO.getSize(queueName);
    }

    /** 从队列中移除任务 */
    public void removeTaskFromQueue(String taskId) {
        Task task = getTask(taskId);
        if (task == null) {
            throw new NotFoundException("No such task found by taskId: %s", taskId);
        }
        queueDAO.remove(QueueUtils.getQueueName(task), taskId);
    }

    /**
     * 重新入队指定类型的待处理任务。
     * 跳过系统任务和终态任务。
     *
     * @return 成功入队的任务数
     */
    public int requeuePendingTasks(String taskType) {

        int count = 0;
        List<Task> tasks = getPendingTasksForTaskType(taskType);

        for (Task pending : tasks) {

            // 跳过系统任务
            if (systemTaskRegistry.isSystemTask(pending.getTaskType())) {
                continue;
            }
            // 跳过终态任务
            if (pending.getStatus().isTerminal()) {
                continue;
            }

            LOGGER.debug(
                    "Requeuing Task: {} of taskType: {} in Workflow: {}",
                    pending.getTaskId(),
                    pending.getTaskType(),
                    pending.getWorkflowInstanceId());
            boolean pushed = requeue(pending);
            if (pushed) {
                count++;
            }
        }
        return count;
    }

    /**
     * 把任务重新入队：计算剩余延迟后 pushIfNotExists。
     * 延迟 = 原 callbackAfterSeconds - 已过去的时间。
     */
    private boolean requeue(Task pending) {
        long callback = pending.getCallbackAfterSeconds();
        if (callback < 0) {
            callback = 0;
        }
        queueDAO.remove(QueueUtils.getQueueName(pending), pending.getTaskId());
        long now = System.currentTimeMillis();
        callback = callback - ((now - pending.getUpdateTime()) / 1000);
        if (callback < 0) {
            callback = 0;
        }
        return queueDAO.pushIfNotExists(
                QueueUtils.getQueueName(pending),
                pending.getTaskId(),
                pending.getWorkflowPriority(),
                callback);
    }

    // ==================== 工作流实例查询 ====================

    /** 按 correlationId 查询工作流实例（可选是否含已关闭、是否含任务） */
    public List<Workflow> getWorkflowInstances(
            String workflowName,
            String correlationId,
            boolean includeClosed,
            boolean includeTasks) {

        List<Workflow> workflows =
                executionDAOFacade.getWorkflowsByCorrelationId(workflowName, correlationId, false);
        return workflows.stream()
                .parallel()
                .filter(
                        workflow -> {
                            // 只保留 RUNNING 或要求包含已关闭的
                            if (includeClosed
                                    || workflow.getStatus()
                                    .equals(Workflow.WorkflowStatus.RUNNING)) {
                                // 仅在需要时加载任务，提升性能
                                if (includeTasks) {
                                    List<Task> tasks =
                                            executionDAOFacade.getTasksForWorkflow(
                                                    workflow.getWorkflowId());
                                    tasks.sort(Comparator.comparingInt(Task::getSeq));
                                    workflow.setTasks(tasks);
                                }
                                return true;
                            } else {
                                return false;
                            }
                        })
                .collect(Collectors.toList());
    }

    /** 获取工作流执行状态 */
    public Workflow getExecutionStatus(String workflowId, boolean includeTasks) {
        return executionDAOFacade.getWorkflow(workflowId, includeTasks);
    }

    /** 获取运行中工作流的 id 列表 */
    public List<String> getRunningWorkflows(String workflowName, int version) {
        return executionDAOFacade.getRunningWorkflowIds(workflowName, version);
    }

    /** 删除工作流（archiveWorkflow=true 时归档） */
    public void removeWorkflow(String workflowId, boolean archiveWorkflow) {
        executionDAOFacade.removeWorkflow(workflowId, archiveWorkflow);
    }

    // ==================== 搜索 ====================

    /** 搜索工作流摘要 */
    public SearchResult<WorkflowSummary> search(
            String query, String freeText, int start, int size, List<String> sortOptions) {
        return executionDAOFacade.searchWorkflowSummary(query, freeText, start, size, sortOptions);
    }

    /** 搜索工作流（完整版）：先搜 id，再并行加载完整对象 */
    public SearchResult<Workflow> searchV2(
            String query, String freeText, int start, int size, List<String> sortOptions) {

        SearchResult<String> result =
                executionDAOFacade.searchWorkflows(query, freeText, start, size, sortOptions);
        List<Workflow> workflows =
                result.getResults().stream()
                        .parallel()
                        .map(
                                workflowId -> {
                                    try {
                                        return executionDAOFacade.getWorkflow(workflowId, false);
                                    } catch (Exception e) {
                                        LOGGER.error(
                                                "Error fetching workflow by id: {}", workflowId, e);
                                        return null;
                                    }
                                })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        // 修正 totalHits（减去加载失败的条数）
        int missing = result.getResults().size() - workflows.size();
        long totalHits = result.getTotalHits() - missing;
        return new SearchResult<>(totalHits, workflows);
    }

    /** 按任务参数搜索工作流（摘要版） */
    public SearchResult<WorkflowSummary> searchWorkflowByTasks(
            String query, String freeText, int start, int size, List<String> sortOptions) {
        SearchResult<TaskSummary> taskSummarySearchResult =
                searchTaskSummary(query, freeText, start, size, sortOptions);
        List<WorkflowSummary> workflowSummaries =
                taskSummarySearchResult.getResults().stream()
                        .parallel()
                        .map(
                                taskSummary -> {
                                    try {
                                        String workflowId = taskSummary.getWorkflowId();
                                        return new WorkflowSummary(
                                                executionDAOFacade.getWorkflow(workflowId, false));
                                    } catch (Exception e) {
                                        LOGGER.error(
                                                "Error fetching workflow by id: {}",
                                                taskSummary.getWorkflowId(),
                                                e);
                                        return null;
                                    }
                                })
                        .filter(Objects::nonNull)
                        .distinct()
                        .collect(Collectors.toList());
        int missing = taskSummarySearchResult.getResults().size() - workflowSummaries.size();
        long totalHits = taskSummarySearchResult.getTotalHits() - missing;
        return new SearchResult<>(totalHits, workflowSummaries);
    }

    /** 按任务参数搜索工作流（完整版） */
    public SearchResult<Workflow> searchWorkflowByTasksV2(
            String query, String freeText, int start, int size, List<String> sortOptions) {
        SearchResult<TaskSummary> taskSummarySearchResult =
                searchTasks(query, freeText, start, size, sortOptions);
        List<Workflow> workflows =
                taskSummarySearchResult.getResults().stream()
                        .parallel()
                        .map(
                                taskSummary -> {
                                    try {
                                        String workflowId = taskSummary.getWorkflowId();
                                        return executionDAOFacade.getWorkflow(workflowId, false);
                                    } catch (Exception e) {
                                        LOGGER.error(
                                                "Error fetching workflow by id: {}",
                                                taskSummary.getWorkflowId(),
                                                e);
                                        return null;
                                    }
                                })
                        .filter(Objects::nonNull)
                        .distinct()
                        .collect(Collectors.toList());
        int missing = taskSummarySearchResult.getResults().size() - workflows.size();
        long totalHits = taskSummarySearchResult.getTotalHits() - missing;
        return new SearchResult<>(totalHits, workflows);
    }

    /** 搜索任务摘要：先搜 id，再并行加载 TaskSummary */
    public SearchResult<TaskSummary> searchTasks(
            String query, String freeText, int start, int size, List<String> sortOptions) {

        SearchResult<String> result =
                executionDAOFacade.searchTasks(query, freeText, start, size, sortOptions);
        List<TaskSummary> workflows =
                result.getResults().stream()
                        .parallel()
                        .map(
                                task -> {
                                    try {
                                        return new TaskSummary(executionDAOFacade.getTask(task));
                                    } catch (Exception e) {
                                        LOGGER.error("Error fetching task by id: {}", task, e);
                                        return null;
                                    }
                                })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        int missing = result.getResults().size() - workflows.size();
        long totalHits = result.getTotalHits() - missing;
        return new SearchResult<>(totalHits, workflows);
    }

    /** 搜索任务摘要（直接委托 DAO） */
    public SearchResult<TaskSummary> searchTaskSummary(
            String query, String freeText, int start, int size, List<String> sortOptions) {
        return executionDAOFacade.searchTaskSummary(query, freeText, start, size, sortOptions);
    }

    /** 搜索任务摘要（sort 以字符串传入） */
    public SearchResult<TaskSummary> getSearchTasks(
            String query,
            String freeText,
            int start,
            int size,
            String sortString) {
        return searchTaskSummary(
                query, freeText, start, size, Utils.convertStringToList(sortString));
    }

    /** 搜索任务完整版（sort 以字符串传入） */
    public SearchResult<Task> getSearchTasksV2(
            String query, String freeText, int start, int size, String sortString) {
        SearchResult<String> result =
                executionDAOFacade.searchTasks(
                        query, freeText, start, size, Utils.convertStringToList(sortString));
        List<Task> tasks =
                result.getResults().stream()
                        .parallel()
                        .map(
                                task -> {
                                    try {
                                        return executionDAOFacade.getTask(task);
                                    } catch (Exception e) {
                                        LOGGER.error("Error fetching task by id: {}", task, e);
                                        return null;
                                    }
                                })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        int missing = result.getResults().size() - tasks.size();
        long totalHits = result.getTotalHits() - missing;
        return new SearchResult<>(totalHits, tasks);
    }

    // ==================== 待处理任务、事件执行、消息、日志 ====================

    /** 查询指定任务类型的待处理任务 */
    public List<Task> getPendingTasksForTaskType(String taskType) {
        return executionDAOFacade.getPendingTasksForTaskType(taskType);
    }

    /** 添加事件执行记录 */
    public boolean addEventExecution(EventExecution eventExecution) {
        return executionDAOFacade.addEventExecution(eventExecution);
    }

    /** 移除事件执行记录 */
    public void removeEventExecution(EventExecution eventExecution) {
        executionDAOFacade.removeEventExecution(eventExecution);
    }

    /** 更新事件执行记录 */
    public void updateEventExecution(EventExecution eventExecution) {
        executionDAOFacade.updateEventExecution(eventExecution);
    }

    /**
     * 向指定队列添加消息。
     *
     * @param queue 队列名
     * @param msg 消息
     */
    public void addMessage(String queue, Message msg) {
        executionDAOFacade.addMessage(queue, msg);
    }

    /**
     * 添加任务执行日志。
     *
     * @param taskId 任务 id
     * @param log 日志内容
     */
    public void log(String taskId, String log) {
        TaskExecLog executionLog = new TaskExecLog();
        executionLog.setTaskId(taskId);
        executionLog.setLog(log);
        executionLog.setCreatedTime(System.currentTimeMillis());
        executionDAOFacade.addTaskExecLog(Collections.singletonList(executionLog));
    }

    /**
     * 获取任务的执行日志。
     *
     * @param taskId 任务 id
     * @return Worker 上报的日志列表
     */
    public List<TaskExecLog> getTaskLogs(String taskId) {
        return executionDAOFacade.getTaskExecutionLogs(taskId);
    }

    /**
     * 获取 payload 的外部存储位置（用于读写大输入/输出）。
     *
     * @param path 路径
     * @param operation 操作（read / write）
     * @param type payload 类型（input / output）
     * @return 外部存储位置
     * @throws IllegalArgumentException 参数非法
     */
    public ExternalStorageLocation getExternalStorageLocation(
            String path, String operation, String type) {
        try {
            ExternalPayloadStorage.Operation payloadOperation =
                    ExternalPayloadStorage.Operation.valueOf(StringUtils.upperCase(operation));
            ExternalPayloadStorage.PayloadType payloadType =
                    ExternalPayloadStorage.PayloadType.valueOf(StringUtils.upperCase(type));
            return externalPayloadStorage.getLocation(payloadOperation, payloadType, path);
        } catch (Exception e) {
            String errorMsg =
                    String.format(
                            "Invalid input - Operation: %s, PayloadType: %s", operation, type);
            LOGGER.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }
    }
}