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
package com.netflix.conductor.core.dal; // 包声明：数据访问层（DAL）包

import java.io.IOException; // 导入 IO 异常类
import java.util.Collections; // 导入集合工具类
import java.util.List; // 导入 List 接口
import java.util.Map; // 导入 Map 接口
import java.util.Objects; // 导入对象工具类
import java.util.concurrent.ScheduledThreadPoolExecutor; // 导入可调度线程池执行器
import java.util.concurrent.TimeUnit; // 导入时间单位枚举
import java.util.stream.Collectors; // 导入流收集器工具类

import javax.annotation.PreDestroy; // 导入 Spring 销毁前回调注解

import org.apache.commons.lang3.StringUtils; // 导入 Apache 字符串工具类
import org.slf4j.Logger; // 导入 SLF4J 日志接口
import org.slf4j.LoggerFactory; // 导入 SLF4J 日志工厂
import org.springframework.stereotype.Component; // 导入 Spring 组件注解

import com.netflix.conductor.common.metadata.events.EventExecution; // 事件执行元数据
import com.netflix.conductor.common.metadata.tasks.PollData; // 任务轮询数据
import com.netflix.conductor.common.metadata.tasks.Task; // 任务通用对象
import com.netflix.conductor.common.metadata.tasks.TaskDef; // 任务定义
import com.netflix.conductor.common.metadata.tasks.TaskExecLog; // 任务执行日志
import com.netflix.conductor.common.run.SearchResult; // 搜索结果封装
import com.netflix.conductor.common.run.TaskSummary; // 任务摘要（用于索引）
import com.netflix.conductor.common.run.Workflow; // 工作流通用对象
import com.netflix.conductor.common.run.WorkflowSummary; // 工作流摘要（用于索引）
import com.netflix.conductor.common.utils.ExternalPayloadStorage; // 外部负载存储接口
import com.netflix.conductor.core.config.ConductorProperties; // Conductor 配置属性
import com.netflix.conductor.core.events.queue.Message; // 事件队列消息
import com.netflix.conductor.core.exception.NotFoundException; // 未找到异常
import com.netflix.conductor.core.exception.TerminateWorkflowException; // 终止工作流异常
import com.netflix.conductor.core.exception.TransientException; // 瞬时（可重试）异常
import com.netflix.conductor.core.utils.ExternalPayloadStorageUtils; // 外部负载存储工具类
import com.netflix.conductor.core.utils.QueueUtils; // 队列工具类
import com.netflix.conductor.dao.*; // 导入所有 DAO 接口
import com.netflix.conductor.metrics.Monitors; // 监控指标上报类
import com.netflix.conductor.model.TaskModel; // 任务模型（内部模型）
import com.netflix.conductor.model.WorkflowModel; // 工作流模型（内部模型）

import com.fasterxml.jackson.core.JsonProcessingException; // Jackson JSON 处理异常
import com.fasterxml.jackson.databind.ObjectMapper; // Jackson 对象映射器

import static com.netflix.conductor.core.utils.Utils.DECIDER_QUEUE; // 静态导入：决策队列名称常量

/**
 * Service that acts as a facade for accessing execution data from the {@link ExecutionDAO}, {@link
 * RateLimitingDAO} and {@link IndexDAO} storage layers
 *
 * 中文说明：该服务作为门面（Facade），统一封装对 ExecutionDAO（执行数据访问）、
 * RateLimitingDAO（限流数据访问）以及 IndexDAO（索引数据访问）等存储层的访问。
 * 上层业务无需直接接触底层多个 DAO，通过本门面完成读写、索引、限流、队列等操作。
 */
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection") // 抑制 Spring 自动装配检查警告
@Component // 声明为 Spring 组件，由容器管理
public class ExecutionDAOFacade {

    // 日志记录器
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionDAOFacade.class);

    private static final String ARCHIVED_FIELD = "archived"; // 索引中"已归档"字段名
    private static final String RAW_JSON_FIELD = "rawJSON"; // 索引中"原始JSON"字段名

    private final ExecutionDAO executionDAO; // 执行数据访问对象（主存储）
    private final QueueDAO queueDAO; // 队列数据访问对象
    private final IndexDAO indexDAO; // 索引数据访问对象（用于搜索/归档）
    private final RateLimitingDAO rateLimitingDao; // 速率限流 DAO
    private final ConcurrentExecutionLimitDAO concurrentExecutionLimitDAO; // 并发执行限制 DAO
    private final PollDataDAO pollDataDAO; // 轮询数据 DAO
    private final ObjectMapper objectMapper; // JSON 序列化/反序列化器
    private final ConductorProperties properties; // Conductor 配置属性
    private final ExternalPayloadStorageUtils externalPayloadStorageUtils; // 外部负载存储工具

    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor; // 延迟索引更新用的调度线程池

    /**
     * 构造方法：通过依赖注入初始化所有 DAO、工具类与线程池。
     */
    public ExecutionDAOFacade(
            ExecutionDAO executionDAO,
            QueueDAO queueDAO,
            IndexDAO indexDAO,
            RateLimitingDAO rateLimitingDao,
            ConcurrentExecutionLimitDAO concurrentExecutionLimitDAO,
            PollDataDAO pollDataDAO,
            ObjectMapper objectMapper,
            ConductorProperties properties,
            ExternalPayloadStorageUtils externalPayloadStorageUtils) {
        this.executionDAO = executionDAO;
        this.queueDAO = queueDAO;
        this.indexDAO = indexDAO;
        this.rateLimitingDao = rateLimitingDao;
        this.concurrentExecutionLimitDAO = concurrentExecutionLimitDAO;
        this.pollDataDAO = pollDataDAO;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.externalPayloadStorageUtils = externalPayloadStorageUtils;
        // 创建 4 个核心线程的调度线程池，用于延迟更新索引
        this.scheduledThreadPoolExecutor =
                new ScheduledThreadPoolExecutor(
                        4,
                        // 拒绝策略：当队列满时记录警告并上报丢弃计数
                        (runnable, executor) -> {
                            LOGGER.warn(
                                    "Request {} to delay updating index dropped in executor {}",
                                    runnable,
                                    executor);
                            Monitors.recordDiscardedIndexingCount("delayQueue");
                        });
        // 设置取消任务时立即从队列移除，避免无用任务占用
        this.scheduledThreadPoolExecutor.setRemoveOnCancelPolicy(true);
    }

    /**
     * Spring 销毁前回调：优雅关闭调度线程池。
     * 先调用 shutdown() 等待已有任务完成，若超时则强制 shutdownNow()。
     */
    @PreDestroy
    public void shutdownExecutorService() {
        try {
            LOGGER.info("Gracefully shutdown executor service");
            scheduledThreadPoolExecutor.shutdown();
            // 等待最多 asyncUpdateDelay 秒，让已提交任务完成
            if (scheduledThreadPoolExecutor.awaitTermination(
                    properties.getAsyncUpdateDelay().getSeconds(), TimeUnit.SECONDS)) {
                LOGGER.debug("tasks completed, shutting down");
            } else {
                // 超时则强制关闭
                LOGGER.warn(
                        "Forcing shutdown after waiting for {} seconds",
                        properties.getAsyncUpdateDelay());
                scheduledThreadPoolExecutor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            // 关闭过程被中断：强制关闭并恢复中断标志
            LOGGER.warn(
                    "Shutdown interrupted, invoking shutdownNow on scheduledThreadPoolExecutor for delay queue");
            scheduledThreadPoolExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 获取工作流模型，并填充其输入/输出以及各任务的输入/输出数据（可能来自外部存储）。
     */
    public WorkflowModel getWorkflowModel(String workflowId, boolean includeTasks) {
        WorkflowModel workflowModel = getWorkflowModelFromDataStore(workflowId, includeTasks);
        populateWorkflowAndTaskPayloadData(workflowModel); // 从外部存储填充负载数据
        return workflowModel;
    }

    /**
     * Fetches the {@link Workflow} object from the data store given the id. Attempts to fetch from
     * {@link ExecutionDAO} first, if not found, attempts to fetch from {@link IndexDAO}.
     *
     * 中文说明：根据工作流 ID 从数据存储获取 {@link Workflow} 对象。
     * 优先从 ExecutionDAO 获取；若未找到，再从 IndexDAO 获取。
     *
     * @param workflowId the id of the workflow to be fetched（要获取的工作流 ID）
     * @param includeTasks if true, fetches the {@link Task} data in the workflow.（是否包含任务数据）
     * @return the {@link Workflow} object（工作流对象）
     * @throws NotFoundException no such {@link Workflow} is found.（未找到工作流时抛出）
     * @throws TransientException parsing the {@link Workflow} object fails.（解析失败时抛出）
     */
    public Workflow getWorkflow(String workflowId, boolean includeTasks) {
        return getWorkflowModelFromDataStore(workflowId, includeTasks).toWorkflow();
    }

    /**
     * 从数据存储中获取 WorkflowModel：
     * 1. 先查 ExecutionDAO；
     * 2. 若为空，再查 IndexDAO 的 rawJSON 字段；
     * 3. 若仍为空，抛出 NotFoundException；
     * 4. 若 JSON 解析失败，抛出 TransientException。
     */
    private WorkflowModel getWorkflowModelFromDataStore(String workflowId, boolean includeTasks) {
        WorkflowModel workflow = executionDAO.getWorkflow(workflowId, includeTasks);
        if (workflow == null) {
            LOGGER.debug("Workflow {} not found in executionDAO, checking indexDAO", workflowId);
            String json = indexDAO.get(workflowId, RAW_JSON_FIELD);
            if (json == null) {
                // 两个存储均无此工作流
                String errorMsg = String.format("No such workflow found by id: %s", workflowId);
                LOGGER.error(errorMsg);
                throw new NotFoundException(errorMsg);
            }

            try {
                workflow = objectMapper.readValue(json, WorkflowModel.class);
                if (!includeTasks) {
                    workflow.getTasks().clear(); // 不需要任务时清空，减少返回体积
                }
            } catch (IOException e) {
                String errorMsg = String.format("Error reading workflow: %s", workflowId);
                LOGGER.error(errorMsg);
                throw new TransientException(errorMsg, e);
            }
        }
        return workflow;
    }

    /**
     * Retrieve all workflow executions with the given correlationId and workflow type Uses the
     * {@link IndexDAO} to search across workflows if the {@link ExecutionDAO} cannot perform
     * searches across workflows.
     *
     * 中文说明：根据 correlationId 与工作流类型查询所有工作流执行。
     * 若 ExecutionDAO 不支持跨工作流搜索，则使用 IndexDAO 搜索。
     *
     * @param workflowName, workflow type to be queried（要查询的工作流类型）
     * @param correlationId the correlation id to be queried（关联 ID）
     * @param includeTasks if true, fetches the {@link Task} data within the workflows（是否包含任务）
     * @return the list of {@link Workflow} executions matching the correlationId（匹配的工作流列表）
     */
    public List<Workflow> getWorkflowsByCorrelationId(
            String workflowName, String correlationId, boolean includeTasks) {
        if (!executionDAO.canSearchAcrossWorkflows()) {
            // ExecutionDAO 不支持跨工作流搜索，走 IndexDAO 查询
            String query =
                    "correlationId='" + correlationId + "' AND workflowType='" + workflowName + "'";
            SearchResult<String> result = indexDAO.searchWorkflows(query, "*", 0, 1000, null);
            // 并行拉取每个工作流详情，忽略 NotFound（可能已被归档清理）
            return result.getResults().stream()
                    .parallel()
                    .map(
                            workflowId -> {
                                try {
                                    return getWorkflow(workflowId, includeTasks);
                                } catch (NotFoundException e) {
                                    // This might happen when the workflow archival failed and the
                                    // workflow was removed from primary datastore
                                    // 中文：当工作流归档失败且已从主存储移除时可能发生
                                    LOGGER.error(
                                            "Error getting the workflow: {}  for correlationId: {} from datastore/index",
                                            workflowId,
                                            correlationId,
                                            e);
                                    return null;
                                }
                            })
                    .filter(Objects::nonNull) // 过滤掉 null
                    .collect(Collectors.toList());
        }
        // 否则直接用 ExecutionDAO 查询
        return executionDAO
                .getWorkflowsByCorrelationId(workflowName, correlationId, includeTasks)
                .stream()
                .map(WorkflowModel::toWorkflow)
                .collect(Collectors.toList());
    }

    /** 根据工作流名称与时间区间查询工作流列表 */
    public List<Workflow> getWorkflowsByName(String workflowName, Long startTime, Long endTime) {
        return executionDAO.getWorkflowsByType(workflowName, startTime, endTime).stream()
                .map(WorkflowModel::toWorkflow)
                .collect(Collectors.toList());
    }

    /** 查询指定名称与版本的待处理（pending）工作流 */
    public List<Workflow> getPendingWorkflowsByName(String workflowName, int version) {
        return executionDAO.getPendingWorkflowsByType(workflowName, version).stream()
                .map(WorkflowModel::toWorkflow)
                .collect(Collectors.toList());
    }

    /** 获取指定名称与版本正在运行的工作流 ID 列表 */
    public List<String> getRunningWorkflowIds(String workflowName, int version) {
        return executionDAO.getRunningWorkflowIds(workflowName, version);
    }

    /** 获取指定名称的待处理工作流数量 */
    public long getPendingWorkflowCount(String workflowName) {
        return executionDAO.getPendingWorkflowCount(workflowName);
    }

    /**
     * Creates a new workflow in the data store
     *
     * 中文说明：在数据存储中创建新工作流，主要步骤：
     * 1. 外部化工作流输入/输出（大负载转存外部存储）；
     * 2. 写入 ExecutionDAO；
     * 3. 推入决策队列（DECIDER_QUEUE），由决策器触发执行；
     * 4. 根据配置同步/异步写入索引。
     *
     * @param workflowModel the workflow to be created（要创建的工作流）
     * @return the id of the created workflow（创建的工作流 ID）
     */
    public String createWorkflow(WorkflowModel workflowModel) {
        externalizeWorkflowData(workflowModel); // 外部化负载
        executionDAO.createWorkflow(workflowModel); // 持久化到主存储
        // Add to decider queue
        // 中文：加入决策队列，等待决策器调度执行
        queueDAO.push(
                DECIDER_QUEUE,
                workflowModel.getWorkflowId(),
                workflowModel.getPriority(),
                properties.getWorkflowOffsetTimeout().getSeconds());
        if (properties.isAsyncIndexingEnabled()) {
            // 异步索引：先返回，由后台线程写索引
            indexDAO.asyncIndexWorkflow(new WorkflowSummary(workflowModel.toWorkflow()));
        } else {
            // 同步索引：立即写入
            indexDAO.indexWorkflow(new WorkflowSummary(workflowModel.toWorkflow()));
        }
        return workflowModel.getWorkflowId();
    }

    /** 将任务的输入/输出负载外部化（转存到外部负载存储） */
    private void externalizeTaskData(TaskModel taskModel) {
        externalPayloadStorageUtils.verifyAndUpload(
                taskModel, ExternalPayloadStorage.PayloadType.TASK_INPUT);
        externalPayloadStorageUtils.verifyAndUpload(
                taskModel, ExternalPayloadStorage.PayloadType.TASK_OUTPUT);
    }

    /** 将工作流的输入/输出负载外部化（转存到外部负载存储） */
    private void externalizeWorkflowData(WorkflowModel workflowModel) {
        externalPayloadStorageUtils.verifyAndUpload(
                workflowModel, ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT);
        externalPayloadStorageUtils.verifyAndUpload(
                workflowModel, ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT);
    }

    /**
     * Updates the given workflow in the data store
     *
     * 中文说明：更新数据存储中的工作流，主要步骤：
     * 1. 更新 updatedTime；若为终态则同时更新 endTime；
     * 2. 外部化负载；
     * 3. 写入 ExecutionDAO；
     * 4. 索引处理：
     *    - 异步索引开启时：若为终态且运行时间短于阈值，延迟更新索引（减少抖动）；
     *      否则立即异步索引；终态时还要异步索引所有任务；
     *    - 异步索引关闭时：同步索引工作流。
     *
     * @param workflowModel the workflow tp be updated（要更新的工作流）
     * @return the id of the updated workflow（被更新工作流的 ID）
     */
    public String updateWorkflow(WorkflowModel workflowModel) {
        workflowModel.setUpdatedTime(System.currentTimeMillis()); // 设置更新时间
        if (workflowModel.getStatus().isTerminal()) {
            workflowModel.setEndTime(System.currentTimeMillis()); // 终态设置结束时间
        }
        externalizeWorkflowData(workflowModel); // 外部化负载
        executionDAO.updateWorkflow(workflowModel); // 更新主存储
        if (properties.isAsyncIndexingEnabled()) {
            // 异步索引逻辑
            if (workflowModel.getStatus().isTerminal()
                    && workflowModel.getEndTime() - workflowModel.getCreateTime()
                    < properties.getAsyncUpdateShortRunningWorkflowDuration().toMillis()) {
                // 短时运行的终态工作流：延迟更新索引，避免频繁写索引造成压力
                final String workflowId = workflowModel.getWorkflowId();
                DelayWorkflowUpdate delayWorkflowUpdate = new DelayWorkflowUpdate(workflowId);
                LOGGER.debug(
                        "Delayed updating workflow: {} in the index by {} seconds",
                        workflowId,
                        properties.getAsyncUpdateDelay());
                scheduledThreadPoolExecutor.schedule(
                        delayWorkflowUpdate,
                        properties.getAsyncUpdateDelay().getSeconds(),
                        TimeUnit.SECONDS);
                // 记录延迟队列大小指标
                Monitors.recordWorkerQueueSize(
                        "delayQueue", scheduledThreadPoolExecutor.getQueue().size());
            } else {
                // 其他情况：立即异步索引
                indexDAO.asyncIndexWorkflow(new WorkflowSummary(workflowModel.toWorkflow()));
            }
            if (workflowModel.getStatus().isTerminal()) {
                // 终态：异步索引所有任务
                workflowModel
                        .getTasks()
                        .forEach(
                                taskModel ->
                                        indexDAO.asyncIndexTask(
                                                new TaskSummary(taskModel.toTask())));
            }
        } else {
            // 同步索引工作流
            indexDAO.indexWorkflow(new WorkflowSummary(workflowModel.toWorkflow()));
        }
        return workflowModel.getWorkflowId();
    }

    /** 从待处理工作流集合中移除指定工作流 */
    public void removeFromPendingWorkflow(String workflowType, String workflowId) {
        executionDAO.removeFromPendingWorkflow(workflowType, workflowId);
    }

    /**
     * Removes the workflow from the data store.
     *
     * 中文说明：从数据存储中移除工作流。步骤：
     * 1. 先取出完整工作流（含任务）；
     * 2. 从 ExecutionDAO 删除工作流；
     * 3. 处理索引（归档或异步删除）；
     * 4. 对每个任务：处理任务索引 + 从对应队列移除；
     * 5. 从决策队列移除该工作流。
     *
     * @param workflowId the id of the workflow to be removed（要移除的工作流 ID）
     * @param archiveWorkflow if true, the workflow and associated tasks will be archived in the
     *     {@link IndexDAO} after removal from {@link ExecutionDAO}.
     *     中文：若为 true，则从 ExecutionDAO 删除后在 IndexDAO 中归档工作流及关联任务。
     */
    public void removeWorkflow(String workflowId, boolean archiveWorkflow) {
        WorkflowModel workflow = getWorkflowModelFromDataStore(workflowId, true); // 取完整工作流

        executionDAO.removeWorkflow(workflowId); // 从主存储删除
        try {
            removeWorkflowIndex(workflow, archiveWorkflow); // 处理工作流索引
        } catch (JsonProcessingException e) {
            throw new TransientException("Workflow can not be serialized to json", e);
        }

        // 遍历所有任务：处理任务索引并清理任务队列
        workflow.getTasks()
                .forEach(
                        task -> {
                            try {
                                removeTaskIndex(workflow, task, archiveWorkflow);
                            } catch (JsonProcessingException e) {
                                throw new TransientException(
                                        String.format(
                                                "Task %s of workflow %s can not be serialized to json",
                                                task.getTaskId(), workflow.getWorkflowId()),
                                        e);
                            }

                            try {
                                // 从任务对应的队列中移除
                                queueDAO.remove(QueueUtils.getQueueName(task), task.getTaskId());
                            } catch (Exception e) {
                                LOGGER.info(
                                        "Error removing task: {} of workflow: {} from {} queue",
                                        workflowId,
                                        task.getTaskId(),
                                        QueueUtils.getQueueName(task),
                                        e);
                            }
                        });

        try {
            // 从决策队列移除该工作流
            queueDAO.remove(DECIDER_QUEUE, workflowId);
        } catch (Exception e) {
            LOGGER.info("Error removing workflow: {} from decider queue", workflowId, e);
        }
    }

    /**
     * 处理工作流索引：
     * - 若归档：要求工作流为终态，同步更新索引的 rawJSON 与 archived 字段（不能异步，避免数据丢失）；
     * - 若不归档：异步从索引中删除。
     */
    private void removeWorkflowIndex(WorkflowModel workflow, boolean archiveWorkflow)
            throws JsonProcessingException {
        if (archiveWorkflow) {
            if (workflow.getStatus().isTerminal()) {
                // Only allow archival if workflow is in terminal state
                // DO NOT archive async, since if archival errors out, workflow data will be lost
                // 中文：仅允许终态工作流归档；不能异步归档，否则归档失败会导致数据丢失
                indexDAO.updateWorkflow(
                        workflow.getWorkflowId(),
                        new String[] {RAW_JSON_FIELD, ARCHIVED_FIELD},
                        new Object[] {objectMapper.writeValueAsString(workflow), true});
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot archive workflow: %s with status: %s",
                                workflow.getWorkflowId(), workflow.getStatus()));
            }
        } else {
            // Not archiving, also remove workflow from index
            // 中文：不归档，则从索引中异步删除
            indexDAO.asyncRemoveWorkflow(workflow.getWorkflowId());
        }
    }

    /**
     * 带过期时间（TTL）地移除工作流：先处理索引，再以 TTL 方式从 ExecutionDAO 删除。
     */
    public void removeWorkflowWithExpiry(
            String workflowId, boolean archiveWorkflow, int ttlSeconds) {
        try {
            WorkflowModel workflow = getWorkflowModelFromDataStore(workflowId, true);

            removeWorkflowIndex(workflow, archiveWorkflow);
            // remove workflow from DAO with TTL
            // 中文：以 TTL 方式从 DAO 删除
            executionDAO.removeWorkflowWithExpiry(workflowId, ttlSeconds);
        } catch (Exception e) {
            Monitors.recordDaoError("executionDao", "removeWorkflow");
            throw new TransientException("Error removing workflow: " + workflowId, e);
        }
    }

    /**
     * Reset the workflow state by removing from the {@link ExecutionDAO} and removing this workflow
     * from the {@link IndexDAO}.
     *
     * 中文说明：重置工作流状态：从 ExecutionDAO 与 IndexDAO 中移除该工作流。
     *
     * @param workflowId the workflow id to be reset（要重置的工作流 ID）
     */
    public void resetWorkflow(String workflowId) {
        getWorkflowModelFromDataStore(workflowId, true); // 先确认存在
        executionDAO.removeWorkflow(workflowId); // 从主存储移除
        try {
            if (properties.isAsyncIndexingEnabled()) {
                indexDAO.asyncRemoveWorkflow(workflowId); // 异步删索引
            } else {
                indexDAO.removeWorkflow(workflowId); // 同步删索引
            }
        } catch (Exception e) {
            throw new TransientException("Error resetting workflow state: " + workflowId, e);
        }
    }

    /** 批量创建任务：先外部化每个任务的负载，再写入 ExecutionDAO */
    public List<TaskModel> createTasks(List<TaskModel> tasks) {
        tasks.forEach(this::externalizeTaskData);
        return executionDAO.createTasks(tasks);
    }

    /** 获取指定工作流下的所有任务（转换为通用 Task 对象） */
    public List<Task> getTasksForWorkflow(String workflowId) {
        return executionDAO.getTasksForWorkflow(workflowId).stream()
                .map(TaskModel::toTask)
                .collect(Collectors.toList());
    }

    /** 获取任务模型，并填充其输入/输出数据（可能来自外部存储） */
    public TaskModel getTaskModel(String taskId) {
        TaskModel taskModel = getTaskFromDatastore(taskId);
        if (taskModel != null) {
            populateTaskData(taskModel); // 填充负载
        }
        return taskModel;
    }

    /** 获取通用 Task 对象（不填充外部负载） */
    public Task getTask(String taskId) {
        TaskModel taskModel = getTaskFromDatastore(taskId);
        if (taskModel != null) {
            return taskModel.toTask();
        }
        return null;
    }

    /** 从 ExecutionDAO 获取任务模型 */
    private TaskModel getTaskFromDatastore(String taskId) {
        return executionDAO.getTask(taskId);
    }

    /** 按任务名称分页查询任务 */
    public List<Task> getTasksByName(String taskName, String startKey, int count) {
        return executionDAO.getTasks(taskName, startKey, count).stream()
                .map(TaskModel::toTask)
                .collect(Collectors.toList());
    }

    /** 获取指定任务类型的待处理任务 */
    public List<Task> getPendingTasksForTaskType(String taskType) {
        return executionDAO.getPendingTasksForTaskType(taskType).stream()
                .map(TaskModel::toTask)
                .collect(Collectors.toList());
    }

    /** 获取指定任务定义下正在执行的任务数量 */
    public long getInProgressTaskCount(String taskDefName) {
        return executionDAO.getInProgressTaskCount(taskDefName);
    }

    /**
     * Sets the update time for the task. Sets the end time for the task (if task is in terminal
     * state and end time is not set). Updates the task in the {@link ExecutionDAO} first, then
     * stores it in the {@link IndexDAO}.
     *
     * 中文说明：更新任务：
     * 1. 根据状态设置 updateTime 与 endTime；
     * 2. 外部化负载；
     * 3. 更新 ExecutionDAO；
     * 4. 索引：
     *    - 异步索引关闭时，每次更新都同步索引任务；
     *    - 异步索引开启时，任务索引推迟到工作流终态时统一处理（减少索引量）。
     *
     * @param taskModel the task to be updated in the data store（要更新的任务）
     * @throws TransientException if the {@link IndexDAO} or {@link ExecutionDAO} operations fail.
     * @throws com.netflix.conductor.core.exception.NonTransientException if the externalization of
     *     payload fails.
     */
    public void updateTask(TaskModel taskModel) {
        if (taskModel.getStatus() != null) {
            // 非终态，或终态但 updateTime 未设置时，刷新 updateTime
            if (!taskModel.getStatus().isTerminal()
                    || (taskModel.getStatus().isTerminal() && taskModel.getUpdateTime() == 0)) {
                taskModel.setUpdateTime(System.currentTimeMillis());
            }
            // 终态且 endTime 未设置时，设置 endTime
            if (taskModel.getStatus().isTerminal() && taskModel.getEndTime() == 0) {
                taskModel.setEndTime(System.currentTimeMillis());
            }
        }
        externalizeTaskData(taskModel); // 外部化负载
        executionDAO.updateTask(taskModel); // 更新主存储
        try {
            /*
             * Indexing a task for every update adds a lot of volume. That is ok but if async indexing
             * is enabled and tasks are stored in memory until a block has completed, we would lose a lot
             * of tasks on a system failure. So only index for each update if async indexing is not enabled.
             * If it *is* enabled, tasks will be indexed only when a workflow is in terminal state.
             *
             * 中文：每次更新都索引任务会带来大量索引写入。若开启异步索引且任务在内存中累积，
             * 系统故障时会丢失大量任务。因此仅在异步索引关闭时每次更新都索引；
             * 异步索引开启时，任务索引统一在工作流终态时进行。
             */
            if (!properties.isAsyncIndexingEnabled()) {
                indexDAO.indexTask(new TaskSummary(taskModel.toTask()));
            }
        } catch (TerminateWorkflowException e) {
            // re-throw it so we can terminate the workflow
            // 中文：直接抛出，以便上层终止工作流
            throw e;
        } catch (Exception e) {
            String errorMsg =
                    String.format(
                            "Error updating task: %s in workflow: %s",
                            taskModel.getTaskId(), taskModel.getWorkflowInstanceId());
            LOGGER.error(errorMsg, e);
            throw new TransientException(errorMsg, e);
        }
    }

    /** 批量更新任务（逐个调用 updateTask） */
    public void updateTasks(List<TaskModel> tasks) {
        tasks.forEach(this::updateTask);
    }

    /** 从 ExecutionDAO 移除任务 */
    public void removeTask(String taskId) {
        executionDAO.removeTask(taskId);
    }

    /**
     * 处理任务索引：
     * - 归档：任务必须为终态，同步更新索引的 archived 字段（不可异步，避免数据丢失）；
     * - 不归档：从索引中异步删除任务。
     */
    private void removeTaskIndex(WorkflowModel workflow, TaskModel task, boolean archiveTask)
            throws JsonProcessingException {
        if (archiveTask) {
            if (task.getStatus().isTerminal()) {
                // Only allow archival if task is in terminal state
                // DO NOT archive async, since if archival errors out, task data will be lost
                // 中文：仅允许终态任务归档；不能异步归档，否则归档失败会导致数据丢失
                indexDAO.updateTask(
                        workflow.getWorkflowId(),
                        task.getTaskId(),
                        new String[] {ARCHIVED_FIELD},
                        new Object[] {true});
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot archive task: %s of workflow: %s with status: %s",
                                task.getTaskId(), workflow.getWorkflowId(), task.getStatus()));
            }
        } else {
            // Not archiving, remove task from index
            // 中文：不归档，则从索引中异步移除任务
            indexDAO.asyncRemoveTask(workflow.getWorkflowId(), task.getTaskId());
        }
    }

    /** 续租任务：刷新 updateTime 并更新 ExecutionDAO（常用于长任务保持租约） */
    public void extendLease(TaskModel taskModel) {
        taskModel.setUpdateTime(System.currentTimeMillis());
        executionDAO.updateTask(taskModel);
    }

    /** 获取指定任务名称的轮询数据 */
    public List<PollData> getTaskPollData(String taskName) {
        return pollDataDAO.getPollData(taskName);
    }

    /** 获取所有轮询数据 */
    public List<PollData> getAllPollData() {
        return pollDataDAO.getAllPollData();
    }

    /** 按任务名与 domain 获取轮询数据；异常时记录日志并返回 null */
    public PollData getTaskPollDataByDomain(String taskName, String domain) {
        try {
            return pollDataDAO.getPollData(taskName, domain);
        } catch (Exception e) {
            LOGGER.error(
                    "Error fetching pollData for task: '{}', domain: '{}'", taskName, domain, e);
            return null;
        }
    }

    /** 更新任务最后一次轮询数据（记录 workerId）；异常时记录日志与监控指标 */
    public void updateTaskLastPoll(String taskName, String domain, String workerId) {
        try {
            pollDataDAO.updateLastPollData(taskName, domain, workerId);
        } catch (Exception e) {
            LOGGER.error(
                    "Error updating PollData for task: {} in domain: {} from worker: {}",
                    taskName,
                    domain,
                    workerId,
                    e);
            Monitors.error(this.getClass().getCanonicalName(), "updateTaskLastPoll");
        }
    }

    /**
     * Save the {@link EventExecution} to the data store Saves to {@link ExecutionDAO} first, if
     * this succeeds then saves to the {@link IndexDAO}.
     *
     * 中文说明：保存事件执行记录。先写入 ExecutionDAO，成功后写入 IndexDAO。
     *
     * @param eventExecution the {@link EventExecution} to be saved（要保存的事件执行）
     * @return true if save succeeds, false otherwise.（保存成功返回 true，否则 false）
     */
    public boolean addEventExecution(EventExecution eventExecution) {
        boolean added = executionDAO.addEventExecution(eventExecution);

        if (added) {
            indexEventExecution(eventExecution); // 主存储成功后再索引
        }

        return added;
    }

    /** 更新事件执行记录：先更新主存储，再写索引 */
    public void updateEventExecution(EventExecution eventExecution) {
        executionDAO.updateEventExecution(eventExecution);
        indexEventExecution(eventExecution);
    }

    /** 根据配置决定是否索引事件执行记录（支持同步/异步） */
    private void indexEventExecution(EventExecution eventExecution) {
        if (properties.isEventExecutionIndexingEnabled()) {
            if (properties.isAsyncIndexingEnabled()) {
                indexDAO.asyncAddEventExecution(eventExecution);
            } else {
                indexDAO.addEventExecution(eventExecution);
            }
        }
    }

    /** 从 ExecutionDAO 移除事件执行记录 */
    public void removeEventExecution(EventExecution eventExecution) {
        executionDAO.removeEventExecution(eventExecution);
    }

    /** 判断任务是否超过并发执行限制 */
    public boolean exceedsInProgressLimit(TaskModel task) {
        return concurrentExecutionLimitDAO.exceedsLimit(task);
    }

    /** 判断任务是否超过速率限制 */
    public boolean exceedsRateLimitPerFrequency(TaskModel task, TaskDef taskDef) {
        return rateLimitingDao.exceedsRateLimitPerFrequency(task, taskDef);
    }

    /**
     * 批量添加任务执行日志：
     * - 未开启索引或日志为空时直接返回；
     * - 记录日志大小指标；
     * - 超过大小限制时截断并告警；
     * - 根据配置同步或异步写入索引。
     */
    public void addTaskExecLog(List<TaskExecLog> logs) {
        if (properties.isTaskExecLogIndexingEnabled() && !logs.isEmpty()) {
            Monitors.recordTaskExecLogSize(logs.size()); // 上报日志大小
            int taskExecLogSizeLimit = properties.getTaskExecLogSizeLimit();
            if (logs.size() > taskExecLogSizeLimit) {
                // 超过限制：截断并告警
                LOGGER.warn(
                        "Task Execution log size: {} for taskId: {} exceeds the limit: {}",
                        logs.size(),
                        logs.get(0).getTaskId(),
                        taskExecLogSizeLimit);
                logs = logs.stream().limit(taskExecLogSizeLimit).collect(Collectors.toList());
            }
            if (properties.isAsyncIndexingEnabled()) {
                indexDAO.asyncAddTaskExecutionLogs(logs);
            } else {
                indexDAO.addTaskExecutionLogs(logs);
            }
        }
    }

    /** 添加消息到指定队列（同步/异步索引） */
    public void addMessage(String queue, Message message) {
        if (properties.isAsyncIndexingEnabled()) {
            indexDAO.asyncAddMessage(queue, message);
        } else {
            indexDAO.addMessage(queue, message);
        }
    }

    /** 搜索工作流 ID */
    public SearchResult<String> searchWorkflows(
            String query, String freeText, int start, int count, List<String> sort) {
        return indexDAO.searchWorkflows(query, freeText, start, count, sort);
    }

    /** 搜索工作流摘要 */
    public SearchResult<WorkflowSummary> searchWorkflowSummary(
            String query, String freeText, int start, int count, List<String> sort) {
        return indexDAO.searchWorkflowSummary(query, freeText, start, count, sort);
    }

    /** 搜索任务 ID */
    public SearchResult<String> searchTasks(
            String query, String freeText, int start, int count, List<String> sort) {
        return indexDAO.searchTasks(query, freeText, start, count, sort);
    }

    /** 搜索任务摘要 */
    public SearchResult<TaskSummary> searchTaskSummary(
            String query, String freeText, int start, int count, List<String> sort) {
        return indexDAO.searchTaskSummary(query, freeText, start, count, sort);
    }

    /** 获取任务执行日志；若未开启索引则返回空列表 */
    public List<TaskExecLog> getTaskExecutionLogs(String taskId) {
        return properties.isTaskExecLogIndexingEnabled()
                ? indexDAO.getTaskExecutionLogs(taskId)
                : Collections.emptyList();
    }

    /**
     * Populates the workflow input data and the tasks input/output data if stored in external
     * payload storage.
     *
     * 中文说明：若工作流或任务的输入/输出存储在外部负载存储中，则下载并填充回模型。
     *
     * @param workflowModel the workflowModel for which the payload data needs to be populated from
     *     external storage (if applicable)
     */
    public void populateWorkflowAndTaskPayloadData(WorkflowModel workflowModel) {
        // 工作流输入负载
        if (StringUtils.isNotBlank(workflowModel.getExternalInputPayloadStoragePath())) {
            Map<String, Object> workflowInputParams =
                    externalPayloadStorageUtils.downloadPayload(
                            workflowModel.getExternalInputPayloadStoragePath());
            Monitors.recordExternalPayloadStorageUsage(
                    workflowModel.getWorkflowName(),
                    ExternalPayloadStorage.Operation.READ.toString(),
                    ExternalPayloadStorage.PayloadType.WORKFLOW_INPUT.toString());
            workflowModel.internalizeInput(workflowInputParams); // 回填到模型
        }

        // 工作流输出负载
        if (StringUtils.isNotBlank(workflowModel.getExternalOutputPayloadStoragePath())) {
            Map<String, Object> workflowOutputParams =
                    externalPayloadStorageUtils.downloadPayload(
                            workflowModel.getExternalOutputPayloadStoragePath());
            Monitors.recordExternalPayloadStorageUsage(
                    workflowModel.getWorkflowName(),
                    ExternalPayloadStorage.Operation.READ.toString(),
                    ExternalPayloadStorage.PayloadType.WORKFLOW_OUTPUT.toString());
            workflowModel.internalizeOutput(workflowOutputParams); // 回填到模型
        }

        // 遍历任务，填充任务输入/输出负载
        workflowModel.getTasks().forEach(this::populateTaskData);
    }

    /** 填充单个任务的输入/输出负载（若外部存储路径存在） */
    public void populateTaskData(TaskModel taskModel) {
        // 任务输出负载
        if (StringUtils.isNotBlank(taskModel.getExternalOutputPayloadStoragePath())) {
            Map<String, Object> outputData =
                    externalPayloadStorageUtils.downloadPayload(
                            taskModel.getExternalOutputPayloadStoragePath());
            taskModel.internalizeOutput(outputData);
            Monitors.recordExternalPayloadStorageUsage(
                    taskModel.getTaskDefName(),
                    ExternalPayloadStorage.Operation.READ.toString(),
                    ExternalPayloadStorage.PayloadType.TASK_OUTPUT.toString());
        }

        // 任务输入负载
        if (StringUtils.isNotBlank(taskModel.getExternalInputPayloadStoragePath())) {
            Map<String, Object> inputData =
                    externalPayloadStorageUtils.downloadPayload(
                            taskModel.getExternalInputPayloadStoragePath());
            taskModel.internalizeInput(inputData);
            Monitors.recordExternalPayloadStorageUsage(
                    taskModel.getTaskDefName(),
                    ExternalPayloadStorage.Operation.READ.toString(),
                    ExternalPayloadStorage.PayloadType.TASK_INPUT.toString());
        }
    }

    /**
     * 延迟更新索引的任务：从 ExecutionDAO 重新获取工作流并异步写入索引。
     * 用于短时运行的终态工作流，避免频繁索引抖动。
     */
    class DelayWorkflowUpdate implements Runnable {

        private final String workflowId; // 要更新的工作流 ID

        DelayWorkflowUpdate(String workflowId) {
            this.workflowId = workflowId;
        }

        @Override
        public void run() {
            try {
                // 重新从 ExecutionDAO 获取（不需要任务，减少开销）
                WorkflowModel workflowModel = executionDAO.getWorkflow(workflowId, false);
                indexDAO.asyncIndexWorkflow(new WorkflowSummary(workflowModel.toWorkflow()));
            } catch (Exception e) {
                LOGGER.error("Unable to update workflow: {}", workflowId, e);
            }
        }
    }
}