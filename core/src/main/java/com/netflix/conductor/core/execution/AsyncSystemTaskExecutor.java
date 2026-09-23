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
package com.netflix.conductor.core.execution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/**
 * 异步系统任务执行器。
 *
 * <p>它是"任务队列"与"decide 决策引擎"之间的桥梁：
 * 后台轮询器（SystemTaskWorkerCoordinator）从队列取出 taskId 后，
 * 交给本类的 {@link #execute(WorkflowSystemTask, String)} 执行。
 *
 * <p>职责：
 * <ul>
 *   <li>加载任务、检查状态（终态/工作流终态/限流）</li>
 *   <li>首次调度调用 systemTask.start()，后续轮询调用 systemTask.execute()</li>
 *   <li>根据任务结果决定：出队 or 延期（postpone）</li>
 *   <li>任务完成后回调 workflowExecutor.decide()，闭环推进工作流</li>
 * </ul>
 *
 * <p>典型异步系统任务：HTTP、SUB_WORKFLOW、EVENT、WAIT 等。
 */
@Component
public class AsyncSystemTaskExecutor {

    /** 执行 DAO 门面：加载/更新工作流和任务 */
    private final ExecutionDAOFacade executionDAOFacade;
    /** 队列 DAO：从队列取任务、延期、出队 */
    private final QueueDAO queueDAO;
    /** 元数据 DAO：按任务定义名查询 TaskDef（用于限流判断） */
    private final MetadataDAO metadataDAO;
    /** 任务消息延期秒数（限流场景下，延期多久再重试） */
    private final long queueTaskMessagePostponeSecs;
    /** 系统任务回调间隔秒数（未完成时，延期多久后再被轮询） */
    private final long systemTaskCallbackTime;
    /** 工作流执行器：任务完成后回调 decide，闭环推进工作流 */
    private final WorkflowExecutor workflowExecutor;

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSystemTaskExecutor.class);

    /**
     * 构造器：注入依赖，并从配置中读取两个关键时间参数。
     *
     * @param executionDAOFacade     执行 DAO 门面
     * @param queueDAO               队列 DAO
     * @param metadataDAO            元数据 DAO
     * @param conductorProperties    Conductor 配置
     * @param workflowExecutor       工作流执行器
     */
    public AsyncSystemTaskExecutor(
            ExecutionDAOFacade executionDAOFacade,
            QueueDAO queueDAO,
            MetadataDAO metadataDAO,
            ConductorProperties conductorProperties,
            WorkflowExecutor workflowExecutor) {
        this.executionDAOFacade = executionDAOFacade;
        this.queueDAO = queueDAO;
        this.metadataDAO = metadataDAO;
        this.workflowExecutor = workflowExecutor;
        // 系统任务回调间隔：未完成时延期多久再被轮询（如 HTTP 等响应的间隔）
        this.systemTaskCallbackTime =
                conductorProperties.getSystemTaskWorkerCallbackDuration().getSeconds();
        // 限流场景下的延期时长：超过并发/频率限制时，延期多久再重试
        this.queueTaskMessagePostponeSecs =
                conductorProperties.getTaskExecutionPostponeDuration().getSeconds();
    }

    /**
     * 执行并持久化一个异步 {@link WorkflowSystemTask} 的结果。
     *
     * <p>这是本类的核心入口，由后台轮询器调用。
     * 内部根据任务状态决定调用 start() 还是 execute()，
     * 并在任务完成时回调 decide() 推进工作流。
     *
     * @param systemTask 要执行的系统任务（如 HttpTask、SubWorkflowTask 等）
     * @param taskId     任务实例 id
     */
    public void execute(WorkflowSystemTask systemTask, String taskId) {
        // 静默加载任务：加载失败（如任务已被删）返回 null，不抛异常
        TaskModel task = loadTaskQuietly(taskId);

        // 任务不存在：清理队列里的"死消息"，避免反复处理同一个无效 taskId
        if (task == null) {
            LOGGER.error("TaskId: {} could not be found while executing {}", taskId, systemTask);
            try {
                LOGGER.debug(
                        "Cleaning up dead task from queue message: taskQueue={}, taskId={}",
                        systemTask.getTaskType(),
                        taskId);
                queueDAO.remove(systemTask.getTaskType(), taskId);
            } catch (Exception e) {
                LOGGER.error(
                        "Failed to remove dead task from queue message: taskQueue={}, taskId={}",
                        systemTask.getTaskType(),
                        taskId);
            }
            return;
        }

        LOGGER.debug("Task: {} fetched from execution DAO for taskId: {}", task, taskId);

        // 计算任务对应的队列名（可能因 domain 不同而不同）
        String queueName = QueueUtils.getQueueName(task);

        // 任务已处于终态：无需执行，直接从队列移除。
        // 注释提到：如果队列积压很大，这种情况是可能发生的（消息重复投递）。
        if (task.getStatus().isTerminal()) {
            LOGGER.info("Task {}/{} was already completed.", task.getTaskType(), task.getTaskId());
            queueDAO.remove(queueName, task.getTaskId());
            return;
        }

        // 仅对 SCHEDULED（首次待执行）的任务做限流检查；
        // IN_PROGRESS（已开始）的任务不再限流，否则会打断正在执行的任务。
        if (task.getStatus().equals(TaskModel.Status.SCHEDULED)) {

            // 并发上限检查：同一任务定义同时在执行的数量是否超限
            if (executionDAOFacade.exceedsInProgressLimit(task)) {
                LOGGER.warn(
                        "Concurrent Execution limited for {}:{}", taskId, task.getTaskDefName());
                // 超限：延期后再重试（背压机制），不硬执行
                postponeQuietly(queueName, task);
                return;
            }

            // 频率上限检查：单位时间内的执行次数是否超限
            if (task.getRateLimitPerFrequency() > 0
                    && executionDAOFacade.exceedsRateLimitPerFrequency(
                    task, metadataDAO.getTaskDef(task.getTaskDefName()))) {
                LOGGER.warn(
                        "RateLimit Execution limited for {}:{}, limit:{}",
                        taskId,
                        task.getTaskDefName(),
                        task.getRateLimitPerFrequency());
                // 超频：延期后再重试
                postponeQuietly(queueName, task);
                return;
            }
        }

        // 标记：本次任务执行是否已完成（决定是否触发 decide）
        boolean hasTaskExecutionCompleted = false;
        // 标记：是否应从队列移除该任务消息
        boolean shouldRemoveTaskFromQueue = false;
        // 任务所属的工作流 id
        String workflowId = task.getWorkflowInstanceId();

        // 注意：无论是否抛异常，finally 里都会 updateTask 持久化，
        // 因为此时 task 对象可能已被修改（状态、输出等）。
        try {
            // 加载工作流（是否带任务取决于 systemTask.isTaskRetrievalRequired()）
            WorkflowModel workflow =
                    executionDAOFacade.getWorkflowModel(
                            workflowId, systemTask.isTaskRetrievalRequired());

            // 工作流已终态：任务无意义，标记 CANCELED 并出队
            if (workflow.getStatus().isTerminal()) {
                LOGGER.info(
                        "Workflow {} has been completed for {}/{}",
                        workflow.toShortString(),
                        systemTask,
                        task.getTaskId());
                if (!task.getStatus().isTerminal()) {
                    task.setStatus(TaskModel.Status.CANCELED);
                    task.setReasonForIncompletion(
                            String.format(
                                    "Workflow is in %s state", workflow.getStatus().toString()));
                }
                shouldRemoveTaskFromQueue = true;
                return;  // finally 里会 updateTask + 出队
            }

            LOGGER.debug(
                    "Executing {}/{} in {} state",
                    task.getTaskType(),
                    task.getTaskId(),
                    task.getStatus());

            // 判断该任务是否为"异步完成"型（如 WAIT、EVENT 等，
            // 它们不是靠轮询推进，而是靠外部信号完成）
            boolean isTaskAsyncComplete = systemTask.isAsyncComplete(task);

            // 增加轮询计数：
            // - SCHEDULED（首次）时增加
            // - 非 asyncComplete 的任务（需要靠轮询推进）每次也增加
            if (task.getStatus() == TaskModel.Status.SCHEDULED || !isTaskAsyncComplete) {
                task.incrementPollCount();
            }

            // 首次调度（SCHEDULED）→ 调用 start()
            // 后续轮询（IN_PROGRESS）→ 调用 execute()
            if (task.getStatus() == TaskModel.Status.SCHEDULED) {
                task.setStartTime(System.currentTimeMillis());
                // 记录队列等待时间指标
                Monitors.recordQueueWaitTime(task.getTaskType(), task.getQueueWaitTime());
                systemTask.start(workflow, task, workflowExecutor);
            } else if (task.getStatus() == TaskModel.Status.IN_PROGRESS) {
                systemTask.execute(workflow, task, workflowExecutor);
            }

            // 根据任务状态决定队列消息的去向：
            // 1) 异步完成型且已不在 SCHEDULED → 出队 + 触发 decide
            // 2) 任务终态 → 记录结束时间 + 出队 + 触发 decide
            // 3) 其他（未完成）→ 延期（postpone），等下一轮再执行
            if (isTaskAsyncComplete && task.getStatus() != TaskModel.Status.SCHEDULED) {
                shouldRemoveTaskFromQueue = true;
                hasTaskExecutionCompleted = true;
            } else if (task.getStatus().isTerminal()) {
                task.setEndTime(System.currentTimeMillis());
                shouldRemoveTaskFromQueue = true;
                hasTaskExecutionCompleted = true;
            } else {
                // 未完成：设置回调间隔，延期后再被轮询
                task.setCallbackAfterSeconds(systemTaskCallbackTime);
                // 允许系统任务自定义评估偏移（如根据响应情况动态调整下次轮询时间）
                systemTask
                        .getEvaluationOffset(task, systemTaskCallbackTime)
                        .ifPresentOrElse(
                                task::setCallbackAfterSeconds,
                                () -> task.setCallbackAfterSeconds(systemTaskCallbackTime));
                // 延期队列消息
                queueDAO.postpone(
                        queueName,
                        task.getTaskId(),
                        task.getWorkflowPriority(),
                        task.getCallbackAfterSeconds());
                LOGGER.debug("{} postponed in queue: {}", task, queueName);
            }

            LOGGER.debug(
                    "Finished execution of {}/{}-{}",
                    systemTask,
                    task.getTaskId(),
                    task.getStatus());

        } catch (Exception e) {
            // 执行异常：记录监控指标和日志。
            // 注意：不向上抛异常，避免打断轮询循环；任务状态由 finally 持久化。
            Monitors.error(AsyncSystemTaskExecutor.class.getSimpleName(), "executeSystemTask");
            LOGGER.error("Error executing system task - {}, with id: {}", systemTask, taskId, e);
        } finally {
            // 无论成功失败，都持久化任务最新状态
            executionDAOFacade.updateTask(task);

            // 需要出队则从队列移除
            if (shouldRemoveTaskFromQueue) {
                queueDAO.remove(queueName, task.getTaskId());
                LOGGER.debug("{} removed from queue: {}", task, queueName);
            }

            // 关键闭环：任务执行完成 → 触发 decide，推进工作流下游
            if (hasTaskExecutionCompleted) {
                workflowExecutor.decide(workflowId);
            }
        }
    }

    /**
     * 静默延期：限流场景下，把任务消息延期后重新入队。
     * 失败只记录日志，不影响主流程（下一轮还会被轮询到）。
     */
    private void postponeQuietly(String queueName, TaskModel task) {
        try {
            queueDAO.postpone(
                    queueName,
                    task.getTaskId(),
                    task.getWorkflowPriority(),
                    queueTaskMessagePostponeSecs);
        } catch (Exception e) {
            LOGGER.error("Error postponing task: {} in queue: {}", task.getTaskId(), queueName);
        }
    }

    /**
     * 静默加载任务：加载失败返回 null，不抛异常。
     * 用于处理"任务已被删除"等边界情况，避免打断轮询。
     */
    private TaskModel loadTaskQuietly(String taskId) {
        try {
            return executionDAOFacade.getTaskModel(taskId);
        } catch (Exception e) {
            return null;
        }
    }
}