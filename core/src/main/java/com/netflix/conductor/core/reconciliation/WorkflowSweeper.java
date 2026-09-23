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
package com.netflix.conductor.core.reconciliation;

import java.time.Instant;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.netflix.conductor.annotations.VisibleForTesting;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.core.WorkflowContext;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.exception.NotFoundException;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.TaskModel.Status;
import com.netflix.conductor.model.WorkflowModel;

import static com.netflix.conductor.core.config.SchedulerConfiguration.SWEEPER_EXECUTOR_NAME;
import static com.netflix.conductor.core.utils.Utils.DECIDER_QUEUE;

/**
 * 工作流清扫器（兜底协调器）。
 *
 * <p>它是 Conductor "持久化执行" 的安全网：事件驱动的 decide 可能因进程崩溃、
 * 事件丢失、异常等原因没有推进工作流，Sweeper 会定期从 DECIDER_QUEUE 中取出
 * 这些"待评估"的工作流，重新调用 decide 让它们继续前进，防止工作流永久卡死。
 *
 * <p>核心职责：
 * <ul>
 *   <li>从 DECIDER_QUEUE 拉取需要评估的 workflowId（由队列的延迟/未确认机制驱动）</li>
 *   <li>加载工作流、修复任务、调用 {@link WorkflowExecutor#decideWithLock} 推进</li>
 *   <li>工作流终态后从 DECIDER_QUEUE 移除</li>
 *   <li>未终态时通过 unack 设置合适的延迟，等下一轮再评估</li>
 * </ul>
 */
@Component
public class WorkflowSweeper {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowSweeper.class);

    private final ConductorProperties properties;
    private final WorkflowExecutor workflowExecutor;
    private final WorkflowRepairService workflowRepairService;
    private final QueueDAO queueDAO;
    private final ExecutionDAOFacade executionDAOFacade;

    private static final String CLASS_NAME = WorkflowSweeper.class.getSimpleName();

    @Autowired
    public WorkflowSweeper(
            WorkflowExecutor workflowExecutor,
            Optional<WorkflowRepairService> workflowRepairService,
            ConductorProperties properties,
            QueueDAO queueDAO,
            ExecutionDAOFacade executionDAOFacade) {
        this.properties = properties;
        this.queueDAO = queueDAO;
        this.workflowExecutor = workflowExecutor;
        this.executionDAOFacade = executionDAOFacade;
        // 修复服务是可选的：没有它时不做任务修复
        this.workflowRepairService = workflowRepairService.orElse(null);
        LOGGER.info("WorkflowSweeper initialized.");
    }

    /**
     * 异步清扫入口：把 sweep 提交到 SWEEPER_EXECUTOR_NAME 线程池执行。
     *
     * @param workflowId 要清扫的工作流 id
     * @return 完成信号（CompletableFuture）
     */
    @Async(SWEEPER_EXECUTOR_NAME)
    public CompletableFuture<Void> sweepAsync(String workflowId) {
        sweep(workflowId);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * 清扫单个工作流：加载 → 修复 → decide → 决定移除还是延迟重扫。
     *
     * @param workflowId 要清扫的工作流 id
     */
    public void sweep(String workflowId) {
        WorkflowModel workflow = null;
        try {
            // 设置工作流上下文（记录是哪个 app 发起的），供 decide 内部使用
            WorkflowContext workflowContext = new WorkflowContext(properties.getAppId());
            WorkflowContext.set(workflowContext);
            LOGGER.debug("Running sweeper for workflow {}", workflowId);

            // 加载工作流（含任务），准备评估
            workflow = executionDAOFacade.getWorkflowModel(workflowId, true);

            if (workflowRepairService != null) {
                // 校验并修复工作流中的任务（处理不一致/丢失的状态）
                workflowRepairService.verifyAndRepairWorkflowTasks(workflow);
            }

            // 加锁执行 decide，推进工作流
            workflow = workflowExecutor.decideWithLock(workflow);
            if (workflow != null && workflow.getStatus().isTerminal()) {
                // 工作流已终态：从 decider 队列移除，不再清扫
                queueDAO.remove(DECIDER_QUEUE, workflowId);
                return;
            }

        } catch (NotFoundException nfe) {
            // 工作流不存在：从队列移除，避免无限重试
            queueDAO.remove(DECIDER_QUEUE, workflowId);
            LOGGER.info(
                    "Workflow NOT found for id:{}. Removed it from decider queue", workflowId, nfe);
            return;
        } catch (Exception e) {
            // 其他异常：记录指标和日志，随后仍走 unack 延迟重试
            Monitors.error(CLASS_NAME, "sweep");
            LOGGER.error("Error running sweep for " + workflowId, e);
        }

        // 计算带抖动的延迟时间，避免大量工作流同时重扫造成尖峰
        long workflowOffsetTimeout =
                workflowOffsetWithJitter(properties.getWorkflowOffsetTimeout().getSeconds());
        if (workflow != null) {
            // 工作流存在但未终态：按任务状态设置合适的 unack 延迟
            long startTime = Instant.now().toEpochMilli();
            unack(workflow, workflowOffsetTimeout);
            long endTime = Instant.now().toEpochMilli();
            Monitors.recordUnackTime(workflow.getWorkflowName(), endTime - startTime);
        } else {
            // 工作流对象为空（异常路径）：直接用 id 设置 unack 延迟
            LOGGER.warn(
                    "Workflow with {} id can not be found. Attempting to unack using the id",
                    workflowId);
            queueDAO.setUnackTimeout(DECIDER_QUEUE, workflowId, workflowOffsetTimeout * 1000);
        }
    }

    /**
     * 根据工作流中任务的当前状态，计算并设置合适的"未确认超时"（unack timeout）。
     *
     * <p>含义：把该工作流在 DECIDER_QUEUE 中的可见时间推迟一段时间，
     * 避免在任务本就需要等待（如 Wait / Human / 响应超时）时过早重扫。
     *
     * @param workflowModel 工作流实例
     * @param workflowOffsetTimeout 默认延迟（秒）
     */
    @VisibleForTesting
    void unack(WorkflowModel workflowModel, long workflowOffsetTimeout) {
        long postponeDurationSeconds = 0;
        for (TaskModel taskModel : workflowModel.getTasks()) {
            if (taskModel.getStatus() == Status.IN_PROGRESS) {
                // 进行中的任务：按任务类型决定延迟
                if (taskModel.getTaskType().equals(TaskType.TASK_TYPE_WAIT)) {
                    // WAIT 任务：有明确超时则按剩余时间，否则用默认延迟
                    if (taskModel.getWaitTimeout() == 0) {
                        postponeDurationSeconds = workflowOffsetTimeout;
                    } else {
                        long deltaInSeconds =
                                (taskModel.getWaitTimeout() - System.currentTimeMillis()) / 1000;
                        postponeDurationSeconds = (deltaInSeconds > 0) ? deltaInSeconds : 0;
                    }
                } else if (taskModel.getTaskType().equals(TaskType.TASK_TYPE_HUMAN)) {
                    // 人工任务：用默认延迟（等人处理）
                    postponeDurationSeconds = workflowOffsetTimeout;
                } else {
                    // 其他任务：有响应超时则超时+1 秒，否则默认延迟
                    postponeDurationSeconds =
                            (taskModel.getResponseTimeoutSeconds() != 0)
                                    ? taskModel.getResponseTimeoutSeconds() + 1
                                    : workflowOffsetTimeout;
                }
                break;
            } else if (taskModel.getStatus() == Status.SCHEDULED) {
                // 已调度但未开始的任务：按任务定义的轮询超时决定延迟
                Optional<TaskDef> taskDefinition = taskModel.getTaskDefinition();
                if (taskDefinition.isPresent()) {
                    TaskDef taskDef = taskDefinition.get();
                    if (taskDef.getPollTimeoutSeconds() != null
                            && taskDef.getPollTimeoutSeconds() != 0) {
                        postponeDurationSeconds = taskDef.getPollTimeoutSeconds() + 1;
                    } else {
                        // 没有轮询超时：用工作流超时+1，否则默认延迟
                        postponeDurationSeconds =
                                (workflowModel.getWorkflowDefinition().getTimeoutSeconds() != 0)
                                        ? workflowModel.getWorkflowDefinition().getTimeoutSeconds()
                                        + 1
                                        : workflowOffsetTimeout;
                    }
                } else {
                    // 任务定义缺失：用工作流超时+1，否则默认延迟
                    postponeDurationSeconds =
                            (workflowModel.getWorkflowDefinition().getTimeoutSeconds() != 0)
                                    ? workflowModel.getWorkflowDefinition().getTimeoutSeconds() + 1
                                    : workflowOffsetTimeout;
                }
                break;
            }
        }
        // 设置未确认超时（毫秒），到期后该工作流才会再次被 sweep
        queueDAO.setUnackTimeout(
                DECIDER_QUEUE, workflowModel.getWorkflowId(), postponeDurationSeconds * 1000);
    }

    /**
     * 为延迟时间加入 ±1/3 的抖动，避免大量工作流在同一时刻集中重扫。
     *
     * <p>例如 workflowOffsetTimeout 为 45 秒时，返回值落在 [30-60] 秒之间。
     *
     * @param workflowOffsetTimeout 基础延迟（秒）
     * @return 加抖动后的延迟（秒）
     */
    @VisibleForTesting
    long workflowOffsetWithJitter(long workflowOffsetTimeout) {
        long range = workflowOffsetTimeout / 3;
        long jitter = new Random().nextInt((int) (2 * range + 1)) - range;
        return workflowOffsetTimeout + jitter;
    }
}