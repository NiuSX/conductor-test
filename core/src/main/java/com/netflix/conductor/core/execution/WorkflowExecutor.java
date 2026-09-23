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

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.annotations.VisibleForTesting;
import com.netflix.conductor.common.metadata.tasks.*;
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.SkipTaskRequest;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.TaskUtils;
import com.netflix.conductor.core.WorkflowContext;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.event.WorkflowCreationEvent;
import com.netflix.conductor.core.event.WorkflowEvaluationEvent;
import com.netflix.conductor.core.exception.*;
import com.netflix.conductor.core.execution.tasks.SystemTaskRegistry;
import com.netflix.conductor.core.execution.tasks.Terminate;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.listener.TaskStatusListener;
import com.netflix.conductor.core.listener.WorkflowStatusListener;
import com.netflix.conductor.core.metadata.MetadataMapperService;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.core.utils.Utils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import com.netflix.conductor.service.ExecutionLockService;

import static com.netflix.conductor.core.utils.Utils.DECIDER_QUEUE;
import static com.netflix.conductor.model.TaskModel.Status.*;

/**
 * 工作流执行器：Conductor 的核心调度组件。
 * 负责工作流的启动、推进（decide）、任务调度、重试、重启、终止、暂停/恢复等。
 * 它是整个"持久化执行引擎"的中枢——每一步状态变更都会持久化，从而支持故障恢复。
 */
@Trace
@Component
public class WorkflowExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowExecutor.class);
    /** 加急优先级：用于把某些工作流推到 decider 队列前面优先评估 */
    private static final int EXPEDITED_PRIORITY = 10;
    private static final String CLASS_NAME = WorkflowExecutor.class.getSimpleName();
    /** 谓词：任务处于"失败/超时等非成功且终态" */
    private static final Predicate<TaskModel> UNSUCCESSFUL_TERMINAL_TASK =
            task -> !task.getStatus().isSuccessful() && task.getStatus().isTerminal();
    /** 谓词：非成功的 JOIN 终态任务（用于重试时特殊处理 JOIN） */
    private static final Predicate<TaskModel> UNSUCCESSFUL_JOIN_TASK =
            UNSUCCESSFUL_TERMINAL_TASK.and(t -> TaskType.TASK_TYPE_JOIN.equals(t.getTaskType()));
    /** 谓词：非终态任务 */
    private static final Predicate<TaskModel> NON_TERMINAL_TASK =
            task -> !task.getStatus().isTerminal();
    private final MetadataDAO metadataDAO;
    private final QueueDAO queueDAO;
    private final DeciderService deciderService;
    private final ConductorProperties properties;
    private final MetadataMapperService metadataMapperService;
    private final ExecutionDAOFacade executionDAOFacade;
    private final ParametersUtils parametersUtils;
    private final IDGenerator idGenerator;
    private final WorkflowStatusListener workflowStatusListener;
    private final TaskStatusListener taskStatusListener;
    private final SystemTaskRegistry systemTaskRegistry;
    private final ApplicationEventPublisher eventPublisher;
    /** worker 最近一次 poll 的超时窗口（毫秒），用于判断 domain 是否活跃 */
    private long activeWorkerLastPollMs;
    private final ExecutionLockService executionLockService;

    /** 谓词：判断某个 poll 记录是否在活跃时间窗口内 */
    private final Predicate<PollData> validateLastPolledTime =
            pollData ->
                    pollData.getLastPollTime()
                            > System.currentTimeMillis() - activeWorkerLastPollMs;

    /** 构造器：注入所有依赖组件 */
    public WorkflowExecutor(
            DeciderService deciderService,
            MetadataDAO metadataDAO,
            QueueDAO queueDAO,
            MetadataMapperService metadataMapperService,
            WorkflowStatusListener workflowStatusListener,
            TaskStatusListener taskStatusListener,
            ExecutionDAOFacade executionDAOFacade,
            ConductorProperties properties,
            ExecutionLockService executionLockService,
            SystemTaskRegistry systemTaskRegistry,
            ParametersUtils parametersUtils,
            IDGenerator idGenerator,
            ApplicationEventPublisher eventPublisher) {
        this.deciderService = deciderService;
        this.metadataDAO = metadataDAO;
        this.queueDAO = queueDAO;
        this.properties = properties;
        this.metadataMapperService = metadataMapperService;
        this.executionDAOFacade = executionDAOFacade;
        this.activeWorkerLastPollMs = properties.getActiveWorkerLastPollTimeout().toMillis();
        this.workflowStatusListener = workflowStatusListener;
        this.taskStatusListener = taskStatusListener;
        this.executionLockService = executionLockService;
        this.parametersUtils = parametersUtils;
        this.idGenerator = idGenerator;
        this.systemTaskRegistry = systemTaskRegistry;
        this.eventPublisher = eventPublisher;
    }

    /**
     * 重置工作流中任务的回调时间。
     * 把处于 SCHEDULED 且 callbackAfterSeconds > 0 的 SIMPLE 任务重置为 0，
     * 让它们可以立即被重新调度。
     *
     * @param workflowId 要重置回调的工作流 id
     * @throws ConflictException 如果工作流已处于终态
     */
    public void resetCallbacksForWorkflow(String workflowId) {
        WorkflowModel workflow = executionDAOFacade.getWorkflowModel(workflowId, true);
        if (workflow.getStatus().isTerminal()) {
            throw new ConflictException(
                    "Workflow is in terminal state. Status = %s", workflow.getStatus());
        }

        // 找出非系统任务、SCHEDULED 状态、且 callbackAfterSeconds > 0 的任务，把回调时间重置为 0
        workflow.getTasks().stream()
                .filter(
                        task ->
                                !systemTaskRegistry.isSystemTask(task.getTaskType())
                                        && SCHEDULED == task.getStatus()
                                        && task.getCallbackAfterSeconds() > 0)
                .forEach(
                        task -> {
                            if (queueDAO.resetOffsetTime(
                                    QueueUtils.getQueueName(task), task.getTaskId())) {
                                task.setCallbackAfterSeconds(0);
                                executionDAOFacade.updateTask(task);
                            }
                        });
    }

    /** 重跑入口：校验参数后调用 rerunWF */
    public String rerun(RerunWorkflowRequest request) {
        Utils.checkNotNull(request.getReRunFromWorkflowId(), "reRunFromWorkflowId is missing");
        if (!rerunWF(
                request.getReRunFromWorkflowId(),
                request.getReRunFromTaskId(),
                request.getTaskInput(),
                request.getWorkflowInput(),
                request.getCorrelationId())) {
            throw new IllegalArgumentException(
                    "Task " + request.getReRunFromTaskId() + " not found");
        }
        return request.getReRunFromWorkflowId();
    }

    /**
     * 重启一个已处于终态的工作流（从头开始，或使用最新定义）。
     * 与 retry 不同：restart 会清空所有任务，重新开始整个工作流。
     *
     * @param workflowId 要重启的工作流 id
     * @param useLatestDefinitions 是否使用最新的工作流/任务定义
     * @throws ConflictException 工作流不在终态
     * @throws NotFoundException 找不到定义，或按定义不可重启
     */
    public void restart(String workflowId, boolean useLatestDefinitions) {
        final WorkflowModel workflow = executionDAOFacade.getWorkflowModel(workflowId, true);

        // 只有终态工作流才能重启
        if (!workflow.getStatus().isTerminal()) {
            String errorMsg =
                    String.format(
                            "Workflow: %s is not in terminal state, unable to restart.", workflow);
            LOGGER.error(errorMsg);
            throw new ConflictException(errorMsg);
        }

        WorkflowDef workflowDef;
        if (useLatestDefinitions) {
            // 使用最新定义
            workflowDef =
                    metadataDAO
                            .getLatestWorkflowDef(workflow.getWorkflowName())
                            .orElseThrow(
                                    () ->
                                            new NotFoundException(
                                                    "Unable to find latest definition for %s",
                                                    workflowId));
            workflow.setWorkflowDefinition(workflowDef);
            workflowDef = metadataMapperService.populateTaskDefinitions(workflowDef);
        } else {
            // 使用工作流启动时固定的定义
            workflowDef =
                    Optional.ofNullable(workflow.getWorkflowDefinition())
                            .orElseGet(
                                    () ->
                                            metadataDAO
                                                    .getWorkflowDef(
                                                            workflow.getWorkflowName(),
                                                            workflow.getWorkflowVersion())
                                                    .orElseThrow(
                                                            () ->
                                                                    new NotFoundException(
                                                                            "Unable to find definition for %s",
                                                                            workflowId)));
        }

        // 如果定义不可重启，且工作流是 COMPLETED，则不允许重启
        if (!workflowDef.isRestartable()
                && workflow.getStatus()
                .equals(
                        WorkflowModel.Status
                                .COMPLETED)) { // Can only restart non-completed workflows
            // when the configuration is set to false
            throw new NotFoundException("Workflow: %s is non-restartable", workflow);
        }

        // 重置主存储中的工作流，并从索引器移除；然后重新创建
        executionDAOFacade.resetWorkflow(workflowId);

        workflow.getTasks().clear();
        workflow.setReasonForIncompletion(null);
        workflow.setFailedTaskId(null);
        workflow.setCreateTime(System.currentTimeMillis());
        workflow.setEndTime(0);
        workflow.setLastRetriedTime(0);
        // 状态改为 RUNNING
        workflow.setStatus(WorkflowModel.Status.RUNNING);
        workflow.setOutput(null);
        workflow.setExternalOutputPayloadStoragePath(null);

        try {
            executionDAOFacade.createWorkflow(workflow);
        } catch (Exception e) {
            Monitors.recordWorkflowStartError(
                    workflowDef.getName(), WorkflowContext.get().getClientApp());
            LOGGER.error("Unable to restart workflow: {}", workflowDef.getName(), e);
            terminateWorkflow(workflowId, "Error when restarting the workflow");
            throw e;
        }

        metadataMapperService.populateWorkflowWithDefinitions(workflow);
        decide(workflowId);

        updateAndPushParents(workflow, "restarted");
    }

    /**
     * 重试一个已失败的终态工作流。
     * 只重试失败/取消的任务，而不是整个工作流（与 restart 区别）。
     *
     * @param workflowId 要重试的工作流 id
     * @param resumeSubworkflowTasks 是否深入子工作流，找到最内层失败的任务再重试
     */
    public void retry(String workflowId, boolean resumeSubworkflowTasks) {
        WorkflowModel workflow = executionDAOFacade.getWorkflowModel(workflowId, true);
        if (!workflow.getStatus().isTerminal()) {
            throw new NotFoundException(
                    "Workflow is still running.  status=%s", workflow.getStatus());
        }
        if (workflow.getTasks().isEmpty()) {
            throw new ConflictException("Workflow has not started yet");
        }

        if (resumeSubworkflowTasks) {
            // 找到第一个非成功终态任务，若是子工作流任务则递归深入
            Optional<TaskModel> taskToRetry =
                    workflow.getTasks().stream().filter(UNSUCCESSFUL_TERMINAL_TASK).findFirst();
            if (taskToRetry.isPresent()) {
                workflow = findLastFailedSubWorkflowIfAny(taskToRetry.get(), workflow);
                retry(workflow);
                updateAndPushParents(workflow, "retried");
            }
        } else {
            retry(workflow);
            updateAndPushParents(workflow, "retried");
        }
    }

    /**
     * 逐级向上更新父工作流中的子工作流任务状态，并推入 decider 队列触发异步评估。
     * 用于子工作流被重试/重启/重跑后，让父工作流感知到变化。
     */
    private void updateAndPushParents(WorkflowModel workflow, String operation) {
        String workflowIdentifier = "";
        while (workflow.hasParent()) {
            // 更新父工作流中的子工作流任务
            TaskModel subWorkflowTask =
                    executionDAOFacade.getTaskModel(workflow.getParentWorkflowTaskId());
            if (subWorkflowTask.getWorkflowTask().isOptional()) {
                // 可选子工作流任务，跳过更新父级
                LOGGER.info(
                        "Sub workflow task {} is optional, skip updating parents", subWorkflowTask);
                break;
            }
            subWorkflowTask.setSubworkflowChanged(true);
            subWorkflowTask.setStatus(IN_PROGRESS);
            executionDAOFacade.updateTask(subWorkflowTask);

            // 添加执行日志
            String currentWorkflowIdentifier = workflow.toShortString();
            workflowIdentifier =
                    !workflowIdentifier.equals("")
                            ? String.format(
                            "%s -> %s", currentWorkflowIdentifier, workflowIdentifier)
                            : currentWorkflowIdentifier;
            TaskExecLog log =
                    new TaskExecLog(
                            String.format("Sub workflow %s %s.", workflowIdentifier, operation));
            log.setTaskId(subWorkflowTask.getTaskId());
            executionDAOFacade.addTaskExecLog(Collections.singletonList(log));
            LOGGER.info("Task {} updated. {}", log.getTaskId(), log.getLog());

            // 把父工作流推入 decider 队列，异步触发 'decide'
            String parentWorkflowId = workflow.getParentWorkflowId();
            WorkflowModel parentWorkflow =
                    executionDAOFacade.getWorkflowModel(parentWorkflowId, true);
            parentWorkflow.setStatus(WorkflowModel.Status.RUNNING);
            parentWorkflow.setLastRetriedTime(System.currentTimeMillis());
            executionDAOFacade.updateWorkflow(parentWorkflow);
            expediteLazyWorkflowEvaluation(parentWorkflowId);

            workflow = parentWorkflow;
        }
    }

    /**
     * 内部 retry 实现：收集可重试任务（失败/超时/取消），把它们重新调度为 SCHEDULED。
     * 特殊处理：CANCELED 的 JOIN / DO_WHILE 任务改为 IN_PROGRESS 并重新入队。
     */
    private void retry(WorkflowModel workflow) {
        // 收集可重试任务：FAILED / FAILED_WITH_TERMINAL_ERROR / TIMED_OUT / CANCELED
        // 但同一 referenceTaskName 若后续已 COMPLETED，则不再重试。
        Map<String, TaskModel> retriableMap = new HashMap<>();
        for (TaskModel task : workflow.getTasks()) {
            switch (task.getStatus()) {
                case FAILED:
                case FAILED_WITH_TERMINAL_ERROR:
                case TIMED_OUT:
                    retriableMap.put(task.getReferenceTaskName(), task);
                    break;
                case CANCELED:
                    if (task.getTaskType().equalsIgnoreCase(TaskType.JOIN.toString())
                            || task.getTaskType().equalsIgnoreCase(TaskType.DO_WHILE.toString())) {
                        // JOIN / DO_WHILE 被取消时特殊处理：改回 IN_PROGRESS 重新入队
                        task.setStatus(IN_PROGRESS);
                        addTaskToQueue(task);
                    } else {
                        retriableMap.put(task.getReferenceTaskName(), task);
                    }
                    break;
                default:
                    // 其他状态（含 COMPLETED）会从重试集合中移除
                    retriableMap.remove(task.getReferenceTaskName());
                    break;
            }
        }

        // 若没有可重试任务，且不是 TIMED_OUT 状态，则抛异常
        if (retriableMap.values().size() == 0
                && workflow.getStatus() != WorkflowModel.Status.TIMED_OUT) {
            throw new ConflictException(
                    "There are no retryable tasks! Use restart if you want to attempt entire workflow execution again.");
        }

        // 更新工作流状态为 RUNNING
        workflow.setStatus(WorkflowModel.Status.RUNNING);
        workflow.setLastRetriedTime(System.currentTimeMillis());
        String lastReasonForIncompletion = workflow.getReasonForIncompletion();
        workflow.setReasonForIncompletion(null);
        // 推入 decider 队列
        queueDAO.push(
                DECIDER_QUEUE,
                workflow.getWorkflowId(),
                workflow.getPriority(),
                properties.getWorkflowOffsetTimeout().getSeconds());
        executionDAOFacade.updateWorkflow(workflow);
        LOGGER.info(
                "Workflow {} that failed due to '{}' was retried",
                workflow.toShortString(),
                lastReasonForIncompletion);

        // 生成重试任务副本（新 taskId、retryCount+1、状态 SCHEDULED）
        final WorkflowModel finalWorkflow = workflow;
        List<TaskModel> retriableTasks =
                retriableMap.values().stream()
                        .sorted(Comparator.comparingInt(TaskModel::getSeq))
                        .map(task -> taskToBeRescheduled(finalWorkflow, task))
                        .collect(Collectors.toList());

        dedupAndAddTasks(workflow, retriableTasks);
        executionDAOFacade.updateTasks(workflow.getTasks());
        scheduleTask(workflow, retriableTasks);
    }

    /**
     * 递归查找最内层失败的子工作流。
     * 如果任务本身是失败的 SUB_WORKFLOW，则进入该子工作流继续查找。
     */
    private WorkflowModel findLastFailedSubWorkflowIfAny(
            TaskModel task, WorkflowModel parentWorkflow) {
        if (TaskType.TASK_TYPE_SUB_WORKFLOW.equals(task.getTaskType())
                && UNSUCCESSFUL_TERMINAL_TASK.test(task)) {
            WorkflowModel subWorkflow =
                    executionDAOFacade.getWorkflowModel(task.getSubWorkflowId(), true);
            Optional<TaskModel> taskToRetry =
                    subWorkflow.getTasks().stream().filter(UNSUCCESSFUL_TERMINAL_TASK).findFirst();
            if (taskToRetry.isPresent()) {
                return findLastFailedSubWorkflowIfAny(taskToRetry.get(), subWorkflow);
            }
        }
        return parentWorkflow;
    }

    /**
     * 把一个失败/取消的任务转换成可重试的新任务：
     * 新 taskId、retryCount+1、状态 SCHEDULED，并重置各种运行时字段。
     *
     * @param task 失败或取消的任务
     * @return 状态为 SCHEDULED 的新任务实例
     */
    private TaskModel taskToBeRescheduled(WorkflowModel workflow, TaskModel task) {
        TaskModel taskToBeRetried = task.copy();
        taskToBeRetried.setTaskId(idGenerator.generate());
        taskToBeRetried.setRetriedTaskId(task.getTaskId());
        taskToBeRetried.setStatus(SCHEDULED);
        taskToBeRetried.setRetryCount(task.getRetryCount() + 1);
        taskToBeRetried.setRetried(false);
        taskToBeRetried.setPollCount(0);
        taskToBeRetried.setCallbackAfterSeconds(0);
        taskToBeRetried.setSubWorkflowId(null);
        taskToBeRetried.setScheduledTime(0);
        taskToBeRetried.setStartTime(0);
        taskToBeRetried.setEndTime(0);
        taskToBeRetried.setWorkerId(null);
        taskToBeRetried.setReasonForIncompletion(null);
        taskToBeRetried.setSeq(0);

        // 为重试任务重新做参数替换
        Map<String, Object> taskInput =
                parametersUtils.getTaskInput(
                        taskToBeRetried.getWorkflowTask().getInputParameters(),
                        workflow,
                        taskToBeRetried.getWorkflowTask().getTaskDefinition(),
                        taskToBeRetried.getTaskId());
        taskToBeRetried.getInputData().putAll(taskInput);

        task.setRetried(true);
        // 原任务生命周期结束
        task.setExecuted(true);
        return taskToBeRetried;
    }

    /**
     * 结束工作流执行：
     * - 若有 TERMINATE 任务，按其 terminationStatus 决定 FAILED 还是 COMPLETED
     * - 否则视为 COMPLETED
     * - 最后取消所有非终态任务
     */
    private void endExecution(WorkflowModel workflow, TaskModel terminateTask) {
        if (terminateTask != null) {
            String terminationStatus =
                    (String)
                            terminateTask
                                    .getInputData()
                                    .get(Terminate.getTerminationStatusParameter());
            String reason =
                    (String)
                            terminateTask
                                    .getInputData()
                                    .get(Terminate.getTerminationReasonParameter());
            if (StringUtils.isBlank(reason)) {
                reason =
                        String.format(
                                "Workflow is %s by TERMINATE task: %s",
                                terminationStatus, terminateTask.getTaskId());
            }
            if (WorkflowModel.Status.FAILED.name().equals(terminationStatus)) {
                workflow.setStatus(WorkflowModel.Status.FAILED);
                workflow =
                        terminate(
                                workflow,
                                new TerminateWorkflowException(
                                        reason, workflow.getStatus(), terminateTask));
            } else {
                workflow.setReasonForIncompletion(reason);
                workflow = completeWorkflow(workflow);
            }
        } else {
            workflow = completeWorkflow(workflow);
        }
        cancelNonTerminalTasks(workflow);
    }

    /**
     * 把工作流标记为 COMPLETED，并更新失败任务名集合、通知监听器、更新父工作流。
     *
     * @param workflow 要完成的工作流
     * @throws ConflictException 如果工作流已是终态
     */
    @VisibleForTesting
    WorkflowModel completeWorkflow(WorkflowModel workflow) {
        LOGGER.debug("Completing workflow execution for {}", workflow.getWorkflowId());

        if (workflow.getStatus().equals(WorkflowModel.Status.COMPLETED)) {
            // 已完成后，从 decider 队列移除，并从 pending 列表移除
            queueDAO.remove(DECIDER_QUEUE, workflow.getWorkflowId());
            executionDAOFacade.removeFromPendingWorkflow(
                    workflow.getWorkflowName(), workflow.getWorkflowId());
            LOGGER.debug("Workflow: {} has already been completed.", workflow.getWorkflowId());
            return workflow;
        }

        if (workflow.getStatus().isTerminal()) {
            String msg =
                    "Workflow is already in terminal state. Current status: "
                            + workflow.getStatus();
            throw new ConflictException(msg);
        }

        deciderService.updateWorkflowOutput(workflow, null);

        workflow.setStatus(WorkflowModel.Status.COMPLETED);

        // 收集失败任务，记录到 failedReferenceTaskNames / failedTaskNames
        List<TaskModel> failedTasks =
                workflow.getTasks().stream()
                        .filter(
                                t ->
                                        FAILED.equals(t.getStatus())
                                                || FAILED_WITH_TERMINAL_ERROR.equals(t.getStatus()))
                        .collect(Collectors.toList());

        workflow.getFailedReferenceTaskNames()
                .addAll(
                        failedTasks.stream()
                                .map(TaskModel::getReferenceTaskName)
                                .collect(Collectors.toSet()));

        workflow.getFailedTaskNames()
                .addAll(
                        failedTasks.stream()
                                .map(TaskModel::getTaskDefName)
                                .collect(Collectors.toSet()));

        executionDAOFacade.updateWorkflow(workflow);
        LOGGER.debug("Completed workflow execution for {}", workflow.getWorkflowId());
        workflowStatusListener.onWorkflowCompletedIfEnabled(workflow);
        Monitors.recordWorkflowCompletion(
                workflow.getWorkflowName(),
                workflow.getEndTime() - workflow.getCreateTime(),
                workflow.getOwnerApp());

        // 若有父工作流，更新父工作流中的子工作流任务，并加急评估父工作流
        if (workflow.hasParent()) {
            updateParentWorkflowTask(workflow);
            LOGGER.info(
                    "{} updated parent {} task {}",
                    workflow.toShortString(),
                    workflow.getParentWorkflowId(),
                    workflow.getParentWorkflowTaskId());
            expediteLazyWorkflowEvaluation(workflow.getParentWorkflowId());
        }

        executionLockService.releaseLock(workflow.getWorkflowId());
        executionLockService.deleteLock(workflow.getWorkflowId());
        return workflow;
    }

    /** 终止工作流（简单入口）：设置状态为 TERMINATED 并调用完整终止逻辑 */
    public void terminateWorkflow(String workflowId, String reason) {
        WorkflowModel workflow = executionDAOFacade.getWorkflowModel(workflowId, true);
        if (WorkflowModel.Status.COMPLETED.equals(workflow.getStatus())) {
            throw new ConflictException("Cannot terminate a COMPLETED workflow.");
        }
        workflow.setStatus(WorkflowModel.Status.TERMINATED);
        terminateWorkflow(workflow, reason, null);
    }

    /**
     * 完整的终止工作流逻辑：加锁、更新状态、写失败任务、通知监听器、
     * 从队列移除任务、更新父工作流、可选的失败工作流触发、取消所有非终态任务。
     *
     * @param workflow 要终止的工作流
     * @param reason 终止原因
     * @param failureWorkflow 失败后要触发的失败工作流（可选）
     */
    public WorkflowModel terminateWorkflow(
            WorkflowModel workflow, String reason, String failureWorkflow) {
        try {
            executionLockService.acquireLock(workflow.getWorkflowId(), 60000);

            if (!workflow.getStatus().isTerminal()) {
                workflow.setStatus(WorkflowModel.Status.TERMINATED);
            }

            try {
                deciderService.updateWorkflowOutput(workflow, null);
            } catch (Exception e) {
                // 更新输出失败不影响终止流程，记录后继续
                LOGGER.error(
                        "Failed to update output data for workflow: {}",
                        workflow.getWorkflowId(),
                        e);
                Monitors.error(CLASS_NAME, "terminateWorkflow");
            }

            // 收集失败任务名
            List<TaskModel> failedTasks =
                    workflow.getTasks().stream()
                            .filter(
                                    t ->
                                            FAILED.equals(t.getStatus())
                                                    || FAILED_WITH_TERMINAL_ERROR.equals(
                                                    t.getStatus()))
                            .collect(Collectors.toList());

            workflow.getFailedReferenceTaskNames()
                    .addAll(
                            failedTasks.stream()
                                    .map(TaskModel::getReferenceTaskName)
                                    .collect(Collectors.toSet()));

            workflow.getFailedTaskNames()
                    .addAll(
                            failedTasks.stream()
                                    .map(TaskModel::getTaskDefName)
                                    .collect(Collectors.toSet()));

            String workflowId = workflow.getWorkflowId();
            workflow.setReasonForIncompletion(reason);
            executionDAOFacade.updateWorkflow(workflow);
            workflowStatusListener.onWorkflowTerminatedIfEnabled(workflow);
            Monitors.recordWorkflowTermination(
                    workflow.getWorkflowName(), workflow.getStatus(), workflow.getOwnerApp());
            LOGGER.info("Workflow {} is terminated because of {}", workflowId, reason);
            List<TaskModel> tasks = workflow.getTasks();
            try {
                // 从任务队列中移除所有任务
                tasks.forEach(
                        task -> queueDAO.remove(QueueUtils.getQueueName(task), task.getTaskId()));
            } catch (Exception e) {
                LOGGER.warn(
                        "Error removing task(s) from queue during workflow termination : {}",
                        workflowId,
                        e);
            }

            // 更新父工作流
            if (workflow.hasParent()) {
                updateParentWorkflowTask(workflow);
                LOGGER.info(
                        "{} updated parent {} task {}",
                        workflow.toShortString(),
                        workflow.getParentWorkflowId(),
                        workflow.getParentWorkflowTaskId());
                expediteLazyWorkflowEvaluation(workflow.getParentWorkflowId());
            }

            // 触发失败工作流（若有配置）
            if (!StringUtils.isBlank(failureWorkflow)) {
                Map<String, Object> input = new HashMap<>(workflow.getInput());
                input.put("workflowId", workflowId);
                input.put("reason", reason);
                input.put("failureStatus", workflow.getStatus().toString());
                if (workflow.getFailedTaskId() != null) {
                    input.put("failureTaskId", workflow.getFailedTaskId());
                }
                input.put("failedWorkflow", workflow);

                try {
                    String failureWFId = idGenerator.generate();
                    StartWorkflowInput startWorkflowInput = new StartWorkflowInput();
                    startWorkflowInput.setName(failureWorkflow);
                    startWorkflowInput.setWorkflowInput(input);
                    startWorkflowInput.setCorrelationId(workflow.getCorrelationId());
                    startWorkflowInput.setTaskToDomain(workflow.getTaskToDomain());
                    startWorkflowInput.setWorkflowId(failureWFId);
                    startWorkflowInput.setTriggeringWorkflowId(workflowId);

                    eventPublisher.publishEvent(new WorkflowCreationEvent(startWorkflowInput));

                    workflow.addOutput("conductor.failure_workflow", failureWFId);
                } catch (Exception e) {
                    LOGGER.error("Failed to start error workflow", e);
                    workflow.getOutput()
                            .put(
                                    "conductor.failure_workflow",
                                    "Error workflow "
                                            + failureWorkflow
                                            + " failed to start.  reason: "
                                            + e.getMessage());
                    Monitors.recordWorkflowStartError(
                            failureWorkflow, WorkflowContext.get().getClientApp());
                }
                executionDAOFacade.updateWorkflow(workflow);
            }
            executionDAOFacade.removeFromPendingWorkflow(
                    workflow.getWorkflowName(), workflow.getWorkflowId());

            // 取消所有非终态任务
            List<String> erroredTasks = cancelNonTerminalTasks(workflow);
            if (!erroredTasks.isEmpty()) {
                throw new NonTransientException(
                        String.format(
                                "Error canceling system tasks: %s",
                                String.join(",", erroredTasks)));
            }
            return workflow;
        } finally {
            executionLockService.releaseLock(workflow.getWorkflowId());
            executionLockService.deleteLock(workflow.getWorkflowId());
        }
    }

    /**
     * Worker 上报任务结果的核心入口：
     * 校验任务状态 → 更新任务 → 更新队列 → 持久化 → 触发 decide 推进工作流。
     *
     * @param taskResult 要更新的任务结果
     * @throws IllegalArgumentException 如果 taskResult 为 null
     * @throws NotFoundException 找不到任务
     */
    public void updateTask(TaskResult taskResult) {
        if (taskResult == null) {
            throw new IllegalArgumentException("Task object is null");
        } else if (taskResult.isExtendLease()) {
            // 延长租约（长任务心跳）
            extendLease(taskResult);
            return;
        }

        String workflowId = taskResult.getWorkflowInstanceId();
        WorkflowModel workflowInstance = executionDAOFacade.getWorkflowModel(workflowId, false);

        TaskModel task =
                Optional.ofNullable(executionDAOFacade.getTaskModel(taskResult.getTaskId()))
                        .orElseThrow(
                                () ->
                                        new NotFoundException(
                                                "No such task found by id: %s",
                                                taskResult.getTaskId()));

        LOGGER.debug("Task: {} belonging to Workflow {} being updated", task, workflowInstance);

        String taskQueueName = QueueUtils.getQueueName(task);

        // 任务已终态：忽略本次更新，从队列移除
        if (task.getStatus().isTerminal()) {
            queueDAO.remove(taskQueueName, taskResult.getTaskId());
            LOGGER.info(
                    "Task: {} has already finished execution with status: {} within workflow: {}. Removed task from queue: {}",
                    task.getTaskId(),
                    task.getStatus(),
                    task.getWorkflowInstanceId(),
                    taskQueueName);
            Monitors.recordUpdateConflict(
                    task.getTaskType(), workflowInstance.getWorkflowName(), task.getStatus());
            return;
        }

        // 工作流已终态：忽略本次更新，从队列移除
        if (workflowInstance.getStatus().isTerminal()) {
            queueDAO.remove(taskQueueName, taskResult.getTaskId());
            LOGGER.info(
                    "Workflow: {} has already finished execution. Task update for: {} ignored and removed from Queue: {}.",
                    workflowInstance,
                    taskResult.getTaskId(),
                    taskQueueName);
            Monitors.recordUpdateConflict(
                    task.getTaskType(),
                    workflowInstance.getWorkflowName(),
                    workflowInstance.getStatus());
            return;
        }

        // 系统任务不设为 SCHEDULED（避免重启系统任务）；worker 任务的 IN_PROGRESS 改为 SCHEDULED 重新入队
        if (!systemTaskRegistry.isSystemTask(task.getTaskType())
                && taskResult.getStatus() == TaskResult.Status.IN_PROGRESS) {
            task.setStatus(SCHEDULED);
        } else {
            task.setStatus(TaskModel.Status.valueOf(taskResult.getStatus().name()));
        }
        task.setOutputMessage(taskResult.getOutputMessage());
        task.setReasonForIncompletion(taskResult.getReasonForIncompletion());
        task.setWorkerId(taskResult.getWorkerId());
        task.setCallbackAfterSeconds(taskResult.getCallbackAfterSeconds());
        task.setOutputData(taskResult.getOutputData());
        task.setSubWorkflowId(taskResult.getSubWorkflowId());

        if (StringUtils.isNotBlank(taskResult.getExternalOutputPayloadStoragePath())) {
            task.setExternalOutputPayloadStoragePath(
                    taskResult.getExternalOutputPayloadStoragePath());
        }

        if (task.getStatus().isTerminal()) {
            task.setEndTime(System.currentTimeMillis());
        }

        // 根据任务状态更新队列中的消息
        switch (task.getStatus()) {
            case COMPLETED:
            case CANCELED:
            case FAILED:
            case FAILED_WITH_TERMINAL_ERROR:
            case TIMED_OUT:
                // 终态：从队列移除
                try {
                    queueDAO.remove(taskQueueName, taskResult.getTaskId());
                    LOGGER.debug(
                            "Task: {} removed from taskQueue: {} since the task status is {}",
                            task,
                            taskQueueName,
                            task.getStatus().name());
                } catch (Exception e) {
                    // 移除失败不影响任务执行，最终会被清理
                    String errorMsg =
                            String.format(
                                    "Error removing the message in queue for task: %s for workflow: %s",
                                    task.getTaskId(), workflowId);
                    LOGGER.warn(errorMsg, e);
                    Monitors.recordTaskQueueOpError(
                            task.getTaskType(), workflowInstance.getWorkflowName());
                }
                break;
            case IN_PROGRESS:
            case SCHEDULED:
                // 延期任务（callbackAfterSeconds）
                try {
                    long callBack = taskResult.getCallbackAfterSeconds();
                    queueDAO.postpone(
                            taskQueueName, task.getTaskId(), task.getWorkflowPriority(), callBack);
                    LOGGER.debug(
                            "Task: {} postponed in taskQueue: {} since the task status is {} with callbackAfterSeconds: {}",
                            task,
                            taskQueueName,
                            task.getStatus().name(),
                            callBack);
                } catch (Exception e) {
                    // postpone 失败会影响任务执行，抛出 TransientException
                    String errorMsg =
                            String.format(
                                    "Error postponing the message in queue for task: %s for workflow: %s",
                                    task.getTaskId(), workflowId);
                    LOGGER.error(errorMsg, e);
                    Monitors.recordTaskQueueOpError(
                            task.getTaskType(), workflowInstance.getWorkflowName());
                    throw new TransientException(errorMsg, e);
                }
                break;
            default:
                break;
        }

        // 持久化任务；失败抛 TransientException 保证一致性
        try {
            executionDAOFacade.updateTask(task);
        } catch (Exception e) {
            String errorMsg =
                    String.format(
                            "Error updating task: %s for workflow: %s",
                            task.getTaskId(), workflowId);
            LOGGER.error(errorMsg, e);
            Monitors.recordTaskUpdateError(task.getTaskType(), workflowInstance.getWorkflowName());
            throw new TransientException(errorMsg, e);
        }

        // 通知任务状态监听器
        try {
            notifyTaskStatusListener(task);
        } catch (Exception e) {
            String errorMsg =
                    String.format(
                            "Error while notifying TaskStatusListener: %s for workflow: %s",
                            task.getTaskId(), workflowId);
            LOGGER.error(errorMsg, e);
        }

        // 写执行日志
        taskResult.getLogs().forEach(taskExecLog -> taskExecLog.setTaskId(task.getTaskId()));
        executionDAOFacade.addTaskExecLog(taskResult.getLogs());

        if (task.getStatus().isTerminal()) {
            long duration = getTaskDuration(0, task);
            long lastDuration = task.getEndTime() - task.getStartTime();
            Monitors.recordTaskExecutionTime(
                    task.getTaskDefName(), duration, true, task.getStatus());
            Monitors.recordTaskExecutionTime(
                    task.getTaskDefName(), lastDuration, false, task.getStatus());
        }

        // 若非延迟评估场景，立即触发 decide 推进工作流
        if (!isLazyEvaluateWorkflow(workflowInstance.getWorkflowDefinition(), task)) {
            decide(workflowId);
        }
    }

    /** 按任务状态通知对应的 TaskStatusListener 回调 */
    private void notifyTaskStatusListener(TaskModel task) {
        switch (task.getStatus()) {
            case COMPLETED:
                taskStatusListener.onTaskCompleted(task);
                break;
            case CANCELED:
                taskStatusListener.onTaskCanceled(task);
                break;
            case FAILED:
                taskStatusListener.onTaskFailed(task);
                break;
            case FAILED_WITH_TERMINAL_ERROR:
                taskStatusListener.onTaskFailedWithTerminalError(task);
                break;
            case TIMED_OUT:
                taskStatusListener.onTaskTimedOut(task);
                break;
            case IN_PROGRESS:
                taskStatusListener.onTaskInProgress(task);
                break;
            case SCHEDULED:
                // no-op, already done in addTaskToQueue
            default:
                break;
        }
    }

    /** 延长任务租约（用于长任务心跳，避免被判定超时） */
    private void extendLease(TaskResult taskResult) {
        TaskModel task =
                Optional.ofNullable(executionDAOFacade.getTaskModel(taskResult.getTaskId()))
                        .orElseThrow(
                                () ->
                                        new NotFoundException(
                                                "No such task found by id: %s",
                                                taskResult.getTaskId()));

        LOGGER.debug(
                "Extend lease for Task: {} belonging to Workflow: {}",
                task,
                task.getWorkflowInstanceId());
        if (!task.getStatus().isTerminal()) {
            try {
                executionDAOFacade.extendLease(task);
            } catch (Exception e) {
                String errorMsg =
                        String.format(
                                "Error extend lease for Task: %s belonging to Workflow: %s",
                                task.getTaskId(), task.getWorkflowInstanceId());
                LOGGER.error(errorMsg, e);
                Monitors.recordTaskExtendLeaseError(task.getTaskType(), task.getWorkflowType());
                throw new TransientException(errorMsg, e);
            }
        }
    }

    /**
     * 判断工作流是否可以延迟评估（lazy evaluate）。
     * 满足以下条件之一返回 true：
     * <ul>
     *   <li>任务是 DO_WHILE 中的循环任务
     *   <li>任务是 FORK_JOIN 分支中的中间任务
     *   <li>任务来自 FORK_JOIN_DYNAMIC
     * </ul>
     *
     * @param workflowDef 工作流定义
     * @param task 正在尝试触发评估的任务
     * @return true 表示可以延迟评估
     */
    @VisibleForTesting
    boolean isLazyEvaluateWorkflow(WorkflowDef workflowDef, TaskModel task) {
        if (task.isLoopOverTask()) {
            return false;
        }

        String taskRefName = task.getReferenceTaskName();
        List<WorkflowTask> workflowTasks = workflowDef.collectTasks();

        List<WorkflowTask> forkTasks =
                workflowTasks.stream()
                        .filter(t -> t.getType().equals(TaskType.FORK_JOIN.name()))
                        .collect(Collectors.toList());

        List<WorkflowTask> joinTasks =
                workflowTasks.stream()
                        .filter(t -> t.getType().equals(TaskType.JOIN.name()))
                        .collect(Collectors.toList());

        if (forkTasks.stream().anyMatch(fork -> fork.has(taskRefName))) {
            return joinTasks.stream().anyMatch(join -> join.getJoinOn().contains(taskRefName))
                    && task.getStatus().isSuccessful();
        }

        return workflowTasks.stream().noneMatch(t -> t.getTaskReferenceName().equals(taskRefName))
                && task.getStatus().isSuccessful();
    }

    /** 按 taskId 获取任务（若有关联的 workflowTask 定义，则填充定义信息） */
    public TaskModel getTask(String taskId) {
        return Optional.ofNullable(executionDAOFacade.getTaskModel(taskId))
                .map(
                        task -> {
                            if (task.getWorkflowTask() != null) {
                                return metadataMapperService.populateTaskWithDefinition(task);
                            }
                            return task;
                        })
                .orElse(null);
    }

    /** 获取指定名称和版本的运行中工作流 */
    public List<Workflow> getRunningWorkflows(String workflowName, int version) {
        return executionDAOFacade.getPendingWorkflowsByName(workflowName, version);
    }

    /** 按名称/版本/时间范围查询工作流 id 列表 */
    public List<String> getWorkflows(String name, Integer version, Long startTime, Long endTime) {
        return executionDAOFacade.getWorkflowsByName(name, startTime, endTime).stream()
                .filter(workflow -> workflow.getWorkflowVersion() == version)
                .map(Workflow::getWorkflowId)
                .collect(Collectors.toList());
    }

    /** 获取运行中工作流的 id 列表 */
    public List<String> getRunningWorkflowIds(String workflowName, int version) {
        return executionDAOFacade.getRunningWorkflowIds(workflowName, version);
    }

    /** 监听 WorkflowEvaluationEvent 事件，异步触发 decide */
    @EventListener(WorkflowEvaluationEvent.class)
    public void handleWorkflowEvaluationEvent(WorkflowEvaluationEvent wee) {
        decide(wee.getWorkflowModel());
    }

    /** 按 workflowId 评估工作流状态（加锁、记录耗时指标） */
    public WorkflowModel decide(String workflowId) {
        StopWatch watch = new StopWatch();
        watch.start();
        if (!executionLockService.acquireLock(workflowId)) {
            return null;
        }
        try {

            WorkflowModel workflow = executionDAOFacade.getWorkflowModel(workflowId, true);
            if (workflow == null) {
                // workflowId 不正确时可能为 null
                return null;
            }
            return decide(workflow);

        } finally {
            executionLockService.releaseLock(workflowId);
            watch.stop();
            Monitors.recordWorkflowDecisionTime(watch.getTime());
        }
    }

    /**
     * decide 的重载：先获取锁，再评估工作流状态。
     *
     * @param workflow 要评估的工作流
     * @return 评估后的工作流
     */
    public WorkflowModel decideWithLock(WorkflowModel workflow) {
        if (workflow == null) {
            return null;
        }
        StopWatch watch = new StopWatch();
        watch.start();
        if (!executionLockService.acquireLock(workflow.getWorkflowId())) {
            return null;
        }
        try {
            return decide(workflow);

        } finally {
            executionLockService.releaseLock(workflow.getWorkflowId());
            watch.stop();
            Monitors.recordWorkflowDecisionTime(watch.getTime());
        }
    }

    /**
     * 工作流状态评估的核心方法（不加锁，由调用方负责加锁）。
     * 流程：
     * 1. 若工作流已终态，取消非终态任务后返回
     * 2. 处理子工作流变化
     * 3. 调用 DeciderService.decide 得到 outcome（完成 / 待调度任务 / 待更新任务）
     * 4. 调度任务、更新任务、持久化
     * 5. 若状态有变化则递归 decide 继续推进
     *
     * @param workflow 要评估的工作流
     * @return 评估后的工作流
     */
    public WorkflowModel decide(WorkflowModel workflow) {
        // 如果工作流已经处于终态（如 COMPLETED、FAILED、TERMINATED 等），则不再继续决策
        if (workflow.getStatus().isTerminal()) {
            // 如果终态不是“成功”状态，则需要取消所有尚未进入终态的任务
            if (!workflow.getStatus().isSuccessful()) {
                cancelNonTerminalTasks(workflow);
            }
            // 终态工作流直接返回，无需后续处理
            return workflow;
        }

        // 处理子工作流变化：重置相关标记，必要时把 JOIN 任务改回 IN_PROGRESS
        // 例如子工作流状态发生变化时，可能需要重新评估父工作流中 JOIN 任务的聚合条件
        adjustStateIfSubWorkflowChanged(workflow);

        try {
            // 调用决策服务，根据当前工作流状态计算出下一步需要做什么
            DeciderService.DeciderOutcome outcome = deciderService.decide(workflow);

            // 如果决策结果表明工作流已经可以结束（所有任务完成或满足终止条件）
            if (outcome.isComplete) {
                // 结束整个工作流执行，terminateTask 表示导致结束的那个任务（可能为 null）
                endExecution(workflow, outcome.terminateTask);
                return workflow;
            }

            // 获取本次决策中需要被调度的任务列表
            List<TaskModel> tasksToBeScheduled = outcome.tasksToBeScheduled;
            // 为这些待调度任务设置所属的 domain（用于任务分发/路由）
            setTaskDomains(tasksToBeScheduled, workflow);

            // 获取本次决策中需要被更新的任务列表
            List<TaskModel> tasksToBeUpdated = outcome.tasksToBeUpdated;

            // 对待调度任务进行去重，并把真正新增的任务加入工作流
            tasksToBeScheduled = dedupAndAddTasks(workflow, tasksToBeScheduled);

            // 调度（启动）这些任务；返回 true 表示工作流状态发生了变化
            boolean stateChanged = scheduleTask(workflow, tasksToBeScheduled); // start

            // 对非异步的系统任务，直接同步执行 start，并加入待更新列表
            // 遍历原始 outcome 中的待调度任务（注意：这里用的是 outcome.tasksToBeScheduled，
            // 而不是去重后的 tasksToBeScheduled，因为去重后的列表可能已过滤掉部分任务）
            for (TaskModel task : outcome.tasksToBeScheduled) {
                // 填充任务数据（如输入参数、上下文等），确保执行时数据完整
                executionDAOFacade.populateTaskData(task);

                // 判断该任务是否为系统任务，且仍处于非终态（即需要执行）
                if (systemTaskRegistry.isSystemTask(task.getTaskType())
                        && NON_TERMINAL_TASK.test(task)) {

                    // 获取对应的系统任务实现
                    WorkflowSystemTask workflowSystemTask =
                            systemTaskRegistry.get(task.getTaskType());

                    // 如果是同步系统任务，则直接在此处执行
                    // execute 返回 true 表示任务执行后状态发生了变化（例如任务完成或推进）
                    if (!workflowSystemTask.isAsync()
                            && workflowSystemTask.execute(workflow, task, this)) {
                        // 将执行后的任务加入待更新列表，以便持久化最新状态
                        tasksToBeUpdated.add(task);
                        // 标记工作流状态已变化，后续会递归继续决策
                        stateChanged = true;
                    }
                }
            }

            // 如果有任务需要更新，或者有任务被调度，则批量更新任务
            if (!outcome.tasksToBeUpdated.isEmpty() || !tasksToBeScheduled.isEmpty()) {
                executionDAOFacade.updateTasks(tasksToBeUpdated);
            }

            // 如果状态有变化，则递归继续 decide，直到工作流达到稳定状态
            // 递归是为了处理同步任务执行后可能引发的新任务调度或状态变更
            if (stateChanged) {
                return decide(workflow);
            }

            // 如果没有状态变化，但仍有任务更新或调度，则更新工作流本身
            if (!outcome.tasksToBeUpdated.isEmpty() || !tasksToBeScheduled.isEmpty()) {
                executionDAOFacade.updateWorkflow(workflow);
            }

            // 返回处理后的工作流对象
            return workflow;

        } catch (TerminateWorkflowException twe) {
            // 终止异常：表示工作流需要被强制终止（例如遇到不可恢复的错误）
            LOGGER.info("Execution terminated of workflow: {}", workflow, twe);
            // 执行终止逻辑，将工作流置为终止状态
            terminate(workflow, twe);
            return workflow;
        } catch (RuntimeException e) {
            // 其他运行时异常：记录错误日志后向上抛出，由上层处理
            LOGGER.error("Error deciding workflow: {}", workflow.getWorkflowId(), e);
            throw e;
        }
    }

    /**
     * 若工作流中有子工作流任务发生变化（subworkflowChanged=true），做相应调整：
     * 重置标记；若定义中含 JOIN / FORK_JOIN_DYNAMIC，则把非成功的 JOIN 终态任务改回 IN_PROGRESS 并重新入队。
     */
    private void adjustStateIfSubWorkflowChanged(WorkflowModel workflow) {
        Optional<TaskModel> changedSubWorkflowTask = findChangedSubWorkflowTask(workflow);
        if (changedSubWorkflowTask.isPresent()) {
            // 重置标记
            TaskModel subWorkflowTask = changedSubWorkflowTask.get();
            subWorkflowTask.setSubworkflowChanged(false);
            executionDAOFacade.updateTask(subWorkflowTask);

            LOGGER.info(
                    "{} reset subworkflowChanged flag for {}",
                    workflow.toShortString(),
                    subWorkflowTask.getTaskId());

            // 若定义含 JOIN 或 FORK_JOIN_DYNAMIC，把非成功的 JOIN 终态任务改回 IN_PROGRESS 重新评估
            if (workflow.getWorkflowDefinition().containsType(TaskType.TASK_TYPE_JOIN)
                    || workflow.getWorkflowDefinition()
                    .containsType(TaskType.TASK_TYPE_FORK_JOIN_DYNAMIC)) {
                workflow.getTasks().stream()
                        .filter(UNSUCCESSFUL_JOIN_TASK)
                        .peek(
                                task -> {
                                    task.setStatus(TaskModel.Status.IN_PROGRESS);
                                    addTaskToQueue(task);
                                })
                        .forEach(executionDAOFacade::updateTask);
            }
        }
    }

    /** 查找第一个 subworkflowChanged=true 且未重试的 SUB_WORKFLOW 任务 */
    private Optional<TaskModel> findChangedSubWorkflowTask(WorkflowModel workflow) {
        WorkflowDef workflowDef =
                Optional.ofNullable(workflow.getWorkflowDefinition())
                        .orElseGet(
                                () ->
                                        metadataDAO
                                                .getWorkflowDef(
                                                        workflow.getWorkflowName(),
                                                        workflow.getWorkflowVersion())
                                                .orElseThrow(
                                                        () ->
                                                                new TransientException(
                                                                        "Workflow Definition is not found")));
        if (workflowDef.containsType(TaskType.TASK_TYPE_SUB_WORKFLOW)
                || workflow.getWorkflowDefinition()
                .containsType(TaskType.TASK_TYPE_FORK_JOIN_DYNAMIC)) {
            return workflow.getTasks().stream()
                    .filter(
                            t ->
                                    t.getTaskType().equals(TaskType.TASK_TYPE_SUB_WORKFLOW)
                                            && t.isSubworkflowChanged()
                                            && !t.isRetried())
                    .findFirst();
        }
        return Optional.empty();
    }

    /**
     * 取消所有非终态任务，并做收尾（通知监听器、从 decider 队列移除）。
     *
     * @return 取消失败的系统任务引用名列表
     */
    @VisibleForTesting
    List<String> cancelNonTerminalTasks(WorkflowModel workflow) {
        List<String> erroredTasks = new ArrayList<>();
        // 把所有非终态任务状态改为 CANCELED；系统任务额外调用 cancel()
        for (TaskModel task : workflow.getTasks()) {
            if (!task.getStatus().isTerminal()) {
                task.setStatus(CANCELED);
                if (systemTaskRegistry.isSystemTask(task.getTaskType())) {
                    WorkflowSystemTask workflowSystemTask =
                            systemTaskRegistry.get(task.getTaskType());
                    try {
                        workflowSystemTask.cancel(workflow, task, this);
                    } catch (Exception e) {
                        erroredTasks.add(task.getReferenceTaskName());
                        LOGGER.error(
                                "Error canceling system task:{}/{} in workflow: {}",
                                workflowSystemTask.getTaskType(),
                                task.getTaskId(),
                                workflow.getWorkflowId(),
                                e);
                    }
                }
                executionDAOFacade.updateTask(task);
            }
        }
        if (erroredTasks.isEmpty()) {
            try {
                workflowStatusListener.onWorkflowFinalizedIfEnabled(workflow);
                queueDAO.remove(DECIDER_QUEUE, workflow.getWorkflowId());
            } catch (Exception e) {
                LOGGER.error(
                        "Error removing workflow: {} from decider queue",
                        workflow.getWorkflowId(),
                        e);
            }
        }
        return erroredTasks;
    }

    /**
     * 去重并添加任务：以 (referenceTaskName + "_" + retryCount) 为唯一键，
     * 避免同一任务被重复调度。
     *
     * @return 实际新增的任务列表
     */
    @VisibleForTesting
    List<TaskModel> dedupAndAddTasks(WorkflowModel workflow, List<TaskModel> tasks) {
        Set<String> tasksInWorkflow =
                workflow.getTasks().stream()
                        .map(task -> task.getReferenceTaskName() + "_" + task.getRetryCount())
                        .collect(Collectors.toSet());

        List<TaskModel> dedupedTasks =
                tasks.stream()
                        .filter(
                                task ->
                                        !tasksInWorkflow.contains(
                                                task.getReferenceTaskName()
                                                        + "_"
                                                        + task.getRetryCount()))
                        .collect(Collectors.toList());

        workflow.getTasks().addAll(dedupedTasks);
        return dedupedTasks;
    }

    /**
     * 暂停工作流：状态改为 PAUSED，并从 decider 队列移除（停止被调度）。
     *
     * @throws ConflictException 工作流已终态
     */
    public void pauseWorkflow(String workflowId) {
        try {
            executionLockService.acquireLock(workflowId, 60000);
            WorkflowModel.Status status = WorkflowModel.Status.PAUSED;
            WorkflowModel workflow = executionDAOFacade.getWorkflowModel(workflowId, false);
            if (workflow.getStatus().isTerminal()) {
                throw new ConflictException(
                        "Workflow %s has ended, status cannot be updated.",
                        workflow.toShortString());
            }
            if (workflow.getStatus().equals(status)) {
                return; // 已暂停
            }
            workflow.setStatus(status);
            executionDAOFacade.updateWorkflow(workflow);
        } finally {
            executionLockService.releaseLock(workflowId);
        }

        // 从 decider 队列移除；失败可忽略，不影响暂停
        try {
            queueDAO.remove(DECIDER_QUEUE, workflowId);
        } catch (Exception e) {
            LOGGER.info(
                    "[pauseWorkflow] Error removing workflow: {} from decider queue",
                    workflowId,
                    e);
        }
    }

    /**
     * 恢复工作流：状态改为 RUNNING，推入 decider 队列并触发 decide。
     *
     * @param workflowId 要恢复的工作流
     * @throws IllegalStateException 工作流不处于 PAUSED 状态
     */
    public void resumeWorkflow(String workflowId) {
        WorkflowModel workflow = executionDAOFacade.getWorkflowModel(workflowId, false);
        if (!workflow.getStatus().equals(WorkflowModel.Status.PAUSED)) {
            throw new IllegalStateException(
                    "The workflow "
                            + workflowId
                            + " is not PAUSED so cannot resume. "
                            + "Current status is "
                            + workflow.getStatus().name());
        }
        workflow.setStatus(WorkflowModel.Status.RUNNING);
        workflow.setLastRetriedTime(System.currentTimeMillis());
        // 推入 decider 队列
        queueDAO.push(
                DECIDER_QUEUE,
                workflow.getWorkflowId(),
                workflow.getPriority(),
                properties.getWorkflowOffsetTimeout().getSeconds());
        executionDAOFacade.updateWorkflow(workflow);
        decide(workflowId);
    }

    /**
     * 跳过工作流中的某个任务：创建一个 SKIPPED 任务替代它，然后触发 decide。
     *
     * @param workflowId 工作流 id
     * @param taskReferenceName 要跳过的任务引用名
     * @param skipTaskRequest 跳过请求（可携带输入/输出）
     * @throws IllegalStateException 工作流未运行、任务不存在或任务已处理
     */
    public void skipTaskFromWorkflow(
            String workflowId, String taskReferenceName, SkipTaskRequest skipTaskRequest) {

        WorkflowModel workflow = executionDAOFacade.getWorkflowModel(workflowId, true);

        // 工作流必须处于 RUNNING 才能跳过任务
        if (!workflow.getStatus().equals(WorkflowModel.Status.RUNNING)) {
            String errorMsg =
                    String.format(
                            "The workflow %s is not running so the task referenced by %s cannot be skipped",
                            workflowId, taskReferenceName);
            throw new IllegalStateException(errorMsg);
        }

        // 任务引用名必须存在于工作流定义中
        WorkflowTask workflowTask =
                workflow.getWorkflowDefinition().getTaskByRefName(taskReferenceName);
        if (workflowTask == null) {
            String errorMsg =
                    String.format(
                            "The task referenced by %s does not exist in the WorkflowDefinition %s",
                            taskReferenceName, workflow.getWorkflowName());
            throw new IllegalStateException(errorMsg);
        }

        // 若任务已经开始处理，则不能跳过
        workflow.getTasks()
                .forEach(
                        task -> {
                            if (task.getReferenceTaskName().equals(taskReferenceName)) {
                                String errorMsg =
                                        String.format(
                                                "The task referenced %s has already been processed, cannot be skipped",
                                                taskReferenceName);
                                throw new IllegalStateException(errorMsg);
                            }
                        });

        // 创建 SKIPPED 任务
        TaskModel taskToBeSkipped = new TaskModel();
        taskToBeSkipped.setTaskId(idGenerator.generate());
        taskToBeSkipped.setReferenceTaskName(taskReferenceName);
        taskToBeSkipped.setWorkflowInstanceId(workflowId);
        taskToBeSkipped.setWorkflowPriority(workflow.getPriority());
        taskToBeSkipped.setStatus(SKIPPED);
        taskToBeSkipped.setEndTime(System.currentTimeMillis());
        taskToBeSkipped.setTaskType(workflowTask.getName());
        taskToBeSkipped.setCorrelationId(workflow.getCorrelationId());
        if (skipTaskRequest != null) {
            taskToBeSkipped.setInputData(skipTaskRequest.getTaskInput());
            taskToBeSkipped.setOutputData(skipTaskRequest.getTaskOutput());
            taskToBeSkipped.setInputMessage(skipTaskRequest.getTaskInputMessage());
            taskToBeSkipped.setOutputMessage(skipTaskRequest.getTaskOutputMessage());
        }
        executionDAOFacade.createTasks(Collections.singletonList(taskToBeSkipped));
        decide(workflow.getWorkflowId());
    }

    /** 获取工作流（可含任务） */
    public WorkflowModel getWorkflow(String workflowId, boolean includeTasks) {
        return executionDAOFacade.getWorkflowModel(workflowId, includeTasks);
    }

    /** 把任务推入对应队列（带 callbackAfterSeconds 则延迟） */
    public void addTaskToQueue(TaskModel task) {
        // put in queue
        String taskQueueName = QueueUtils.getQueueName(task);
        if (task.getCallbackAfterSeconds() > 0) {
            queueDAO.push(
                    taskQueueName,
                    task.getTaskId(),
                    task.getWorkflowPriority(),
                    task.getCallbackAfterSeconds());
        } else {
            queueDAO.push(taskQueueName, task.getTaskId(), task.getWorkflowPriority(), 0);
        }
        LOGGER.debug(
                "Added task {} with priority {} to queue {} with call back seconds {}",
                task,
                task.getWorkflowPriority(),
                taskQueueName,
                task.getCallbackAfterSeconds());
    }

    /**
     * 根据 taskToDomain 配置设置任务的目标 domain。
     * Step 1：应用 "*" 通配映射
     * Step 2：应用任务类型特定的映射覆盖
     */
    @VisibleForTesting
    void setTaskDomains(List<TaskModel> tasks, WorkflowModel workflow) {
        Map<String, String> taskToDomain = workflow.getTaskToDomain();
        if (taskToDomain != null) {
            // Step 1: 先应用 "*" 映射到所有任务
            String domainstr = taskToDomain.get("*");
            if (StringUtils.isNotBlank(domainstr)) {
                String[] domains = domainstr.split(",");
                tasks.forEach(
                        task -> {
                            // 过滤系统任务
                            if (!systemTaskRegistry.isSystemTask(task.getTaskType())) {
                                // 选择有活跃 worker 的 domain
                                task.setDomain(getActiveDomain(task.getTaskType(), domains));
                            }
                        });
            }
            // Step 2: 应用任务类型特定映射覆盖
            tasks.forEach(
                    task -> {
                        if (!systemTaskRegistry.isSystemTask(task.getTaskType())) {
                            String taskDomainstr = taskToDomain.get(task.getTaskType());
                            if (taskDomainstr != null) {
                                task.setDomain(
                                        getActiveDomain(
                                                task.getTaskType(), taskDomainstr.split(",")));
                            }
                        }
                    });
        }
    }

    /**
     * 从 domain 列表中选择活跃 domain：
     * 依次检查每个 domain 是否在最近 activeWorkerLastPollMs 内有 worker poll，
     * 找到即返回；若都没有：
     * <li>若列表含 NO_DOMAIN 则返回 null
     * <li>否则返回列表最后一个 domain
     *
     * @param taskType 任务类型
     * @param domains domain 数组（至少一个元素）
     * @return 选中的活跃 domain
     */
    @VisibleForTesting
    String getActiveDomain(String taskType, String[] domains) {
        if (domains == null || domains.length == 0) {
            return null;
        }

        return Arrays.stream(domains)
                .filter(domain -> !domain.equalsIgnoreCase("NO_DOMAIN"))
                .map(domain -> executionDAOFacade.getTaskPollDataByDomain(taskType, domain.trim()))
                .filter(Objects::nonNull)
                .filter(validateLastPolledTime)
                .findFirst()
                .map(PollData::getDomain)
                .orElse(
                        domains[domains.length - 1].trim().equalsIgnoreCase("NO_DOMAIN")
                                ? null
                                : domains[domains.length - 1].trim());
    }

    /** 递归计算任务总耗时（含被重试的前序任务） */
    private long getTaskDuration(long s, TaskModel task) {
        long duration = task.getEndTime() - task.getStartTime();
        s += duration;
        if (task.getRetriedTaskId() == null) {
            return s;
        }
        return s + getTaskDuration(s, executionDAOFacade.getTaskModel(task.getRetriedTaskId()));
    }

    /**
     * 调度任务：为任务分配 seq、持久化、区分系统任务/worker 任务。
     * - 系统任务：同步的立即 start 并更新；异步的入队
     * - worker 任务：直接入队
     *
     * @return 是否有系统任务被启动
     */
    @VisibleForTesting
    boolean scheduleTask(WorkflowModel workflow, List<TaskModel> tasks) {
        List<TaskModel> tasksToBeQueued;
        boolean startedSystemTasks = false;

        try {
            if (tasks == null || tasks.isEmpty()) {
                return false;
            }

            // 取当前最大 seq
            int count = workflow.getTasks().stream().mapToInt(TaskModel::getSeq).max().orElse(0);

            for (TaskModel task : tasks) {
                if (task.getSeq() == 0) { // 仅当 seq 未设置时才分配
                    task.setSeq(++count);
                }
            }

            // 记录工作流内任务数分布指标
            Monitors.recordNumTasksInWorkflow(
                    workflow.getTasks().size() + tasks.size(),
                    workflow.getWorkflowName(),
                    String.valueOf(workflow.getWorkflowVersion()));

            // 持久化任务
            executionDAOFacade.createTasks(tasks);

            List<TaskModel> systemTasks =
                    tasks.stream()
                            .filter(task -> systemTaskRegistry.isSystemTask(task.getTaskType()))
                            .collect(Collectors.toList());

            tasksToBeQueued =
                    tasks.stream()
                            .filter(task -> !systemTaskRegistry.isSystemTask(task.getTaskType()))
                            .collect(Collectors.toList());

            // 遍历系统任务：同步的立即 start，异步的入队
            for (TaskModel task : systemTasks) {
                WorkflowSystemTask workflowSystemTask = systemTaskRegistry.get(task.getTaskType());
                if (workflowSystemTask == null) {
                    throw new NotFoundException(
                            "No system task found by name %s", task.getTaskType());
                }
                if (task.getStatus() != null
                        && !task.getStatus().isTerminal()
                        && task.getStartTime() == 0) {
                    task.setStartTime(System.currentTimeMillis());
                }
                if (!workflowSystemTask.isAsync()) {
                    try {
                        // 启动同步系统任务
                        workflowSystemTask.start(workflow, task, this);
                    } catch (Exception e) {
                        String errorMsg =
                                String.format(
                                        "Unable to start system task: %s, {id: %s, name: %s}",
                                        task.getTaskType(),
                                        task.getTaskId(),
                                        task.getTaskDefName());
                        throw new NonTransientException(errorMsg, e);
                    }
                    startedSystemTasks = true;
                    executionDAOFacade.updateTask(task);
                } else {
                    tasksToBeQueued.add(task);
                }
            }

        } catch (Exception e) {
            List<String> taskIds =
                    tasks.stream().map(TaskModel::getTaskId).collect(Collectors.toList());
            String errorMsg =
                    String.format(
                            "Error scheduling tasks: %s, for workflow: %s",
                            taskIds, workflow.getWorkflowId());
            LOGGER.error(errorMsg, e);
            Monitors.error(CLASS_NAME, "scheduleTask");
            throw new TerminateWorkflowException(errorMsg);
        }

        // 入队失败时忽略异常，交由 WorkflowRepairService 后续补发
        try {
            addTaskToQueue(tasksToBeQueued);
        } catch (Exception e) {
            List<String> taskIds =
                    tasksToBeQueued.stream().map(TaskModel::getTaskId).collect(Collectors.toList());
            String errorMsg =
                    String.format(
                            "Error pushing tasks to the queue: %s, for workflow: %s",
                            taskIds, workflow.getWorkflowId());
            LOGGER.warn(errorMsg, e);
            Monitors.error(CLASS_NAME, "scheduleTask");
        }
        return startedSystemTasks;
    }

    /** 批量入队，并通知 TaskStatusListener.onTaskScheduled */
    private void addTaskToQueue(final List<TaskModel> tasks) {
        for (TaskModel task : tasks) {
            addTaskToQueue(task);
            // 通知 TaskStatusListener
            try {
                taskStatusListener.onTaskScheduled(task);
            } catch (Exception e) {
                String errorMsg =
                        String.format(
                                "Error while notifying TaskStatusListener: %s for workflow: %s",
                                task.getTaskId(), task.getWorkflowInstanceId());
                LOGGER.error(errorMsg, e);
            }
        }
    }

    /** 终止工作流内部实现：设置状态、记录失败任务、触发失败工作流 */
    private WorkflowModel terminate(
            final WorkflowModel workflow, TerminateWorkflowException terminateWorkflowException) {
        if (!workflow.getStatus().isTerminal()) {
            workflow.setStatus(terminateWorkflowException.getWorkflowStatus());
        }

        if (terminateWorkflowException.getTask() != null && workflow.getFailedTaskId() == null) {
            workflow.setFailedTaskId(terminateWorkflowException.getTask().getTaskId());
        }

        String failureWorkflow = workflow.getWorkflowDefinition().getFailureWorkflow();
        if (failureWorkflow != null) {
            if (failureWorkflow.startsWith("$")) {
                // 支持从输入参数中动态解析失败工作流名
                String[] paramPathComponents = failureWorkflow.split("\\.");
                String name = paramPathComponents[2]; // name of the input parameter
                failureWorkflow = (String) workflow.getInput().get(name);
            }
        }
        if (terminateWorkflowException.getTask() != null) {
            executionDAOFacade.updateTask(terminateWorkflowException.getTask());
        }
        return terminateWorkflow(
                workflow, terminateWorkflowException.getMessage(), failureWorkflow);
    }

    /**
     * 重跑工作流的核心实现：
     * - taskId 为 null：重跑整个工作流（清空所有任务）
     * - taskId 非 null：从指定任务开始重跑（删除该任务之后的所有任务）
     *
     * @return 是否成功找到并重跑
     */
    private boolean rerunWF(
            String workflowId,
            String taskId,
            Map<String, Object> taskInput,
            Map<String, Object> workflowInput,
            String correlationId) {

        // 获取工作流
        WorkflowModel workflow = executionDAOFacade.getWorkflowModel(workflowId, true);
        if (!workflow.getStatus().isTerminal()) {
            String errorMsg =
                    String.format(
                            "Workflow: %s is not in terminal state, unable to rerun.", workflow);
            LOGGER.error(errorMsg);
            throw new ConflictException(errorMsg);
        }
        updateAndPushParents(workflow, "reran");

        // taskId 为 null：重跑整个工作流
        if (taskId == null) {
            // 移除所有任务
            workflow.getTasks().forEach(task -> executionDAOFacade.removeTask(task.getTaskId()));
            workflow.setTasks(new ArrayList<>());
            // 状态设为 RUNNING
            workflow.setStatus(WorkflowModel.Status.RUNNING);
            // 重置失败信息
            workflow.setReasonForIncompletion(null);
            workflow.setFailedTaskId(null);
            workflow.setFailedReferenceTaskNames(new HashSet<>());
            workflow.setFailedTaskNames(new HashSet<>());

            if (correlationId != null) {
                workflow.setCorrelationId(correlationId);
            }
            if (workflowInput != null) {
                workflow.setInput(workflowInput);
            }

            queueDAO.push(
                    DECIDER_QUEUE,
                    workflow.getWorkflowId(),
                    workflow.getPriority(),
                    properties.getWorkflowOffsetTimeout().getSeconds());
            executionDAOFacade.updateWorkflow(workflow);

            decide(workflowId);
            return true;
        }

        // 查找指定任务
        TaskModel rerunFromTask = null;
        for (TaskModel task : workflow.getTasks()) {
            if (task.getTaskId().equals(taskId)) {
                rerunFromTask = task;
                break;
            }
        }

        // 若当前工作流找不到，尝试在子工作流中递归查找
        if (rerunFromTask == null) {
            for (TaskModel task : workflow.getTasks()) {
                if (task.getTaskType().equalsIgnoreCase(TaskType.TASK_TYPE_SUB_WORKFLOW)) {
                    String subWorkflowId = task.getSubWorkflowId();
                    if (rerunWF(subWorkflowId, taskId, taskInput, null, null)) {
                        rerunFromTask = task;
                        break;
                    }
                }
            }
        }

        if (rerunFromTask != null) {
            // 状态设为 RUNNING
            workflow.setStatus(WorkflowModel.Status.RUNNING);
            // 重置失败信息
            workflow.setReasonForIncompletion(null);
            workflow.setFailedTaskId(null);
            workflow.setFailedReferenceTaskNames(new HashSet<>());
            workflow.setFailedTaskNames(new HashSet<>());

            if (correlationId != null) {
                workflow.setCorrelationId(correlationId);
            }
            if (workflowInput != null) {
                workflow.setInput(workflowInput);
            }
            // 推入 decider 队列
            queueDAO.push(
                    DECIDER_QUEUE,
                    workflow.getWorkflowId(),
                    workflow.getPriority(),
                    properties.getWorkflowOffsetTimeout().getSeconds());
            executionDAOFacade.updateWorkflow(workflow);
            // 更新任务以修复 workflow-tasks 关系（归档工作流场景）
            executionDAOFacade.updateTasks(workflow.getTasks());
            // 移除 rerunFromTask 之后的所有任务
            List<TaskModel> filteredTasks = new ArrayList<>();
            for (TaskModel task : workflow.getTasks()) {
                if (task.getSeq() > rerunFromTask.getSeq()) {
                    executionDAOFacade.removeTask(task.getTaskId());
                } else {
                    filteredTasks.add(task);
                }
            }
            workflow.setTasks(filteredTasks);
            // 重置任务字段
            rerunFromTask.setScheduledTime(System.currentTimeMillis());
            rerunFromTask.setStartTime(0);
            rerunFromTask.setUpdateTime(0);
            rerunFromTask.setEndTime(0);
            rerunFromTask.clearOutput();
            rerunFromTask.setRetried(false);
            rerunFromTask.setExecuted(false);
            if (rerunFromTask.getTaskType().equalsIgnoreCase(TaskType.TASK_TYPE_SUB_WORKFLOW)) {
                // 子工作流任务：置为 IN_PROGRESS 并重置开始时间
                rerunFromTask.setStatus(IN_PROGRESS);
                rerunFromTask.setStartTime(System.currentTimeMillis());
            } else {
                if (taskInput != null) {
                    rerunFromTask.setInputData(taskInput);
                }
                if (systemTaskRegistry.isSystemTask(rerunFromTask.getTaskType())
                        && !systemTaskRegistry.get(rerunFromTask.getTaskType()).isAsync()) {
                    // 同步系统任务：直接 start
                    systemTaskRegistry
                            .get(rerunFromTask.getTaskType())
                            .start(workflow, rerunFromTask, this);
                } else {
                    // 其他任务：置为 SCHEDULED 并入队
                    rerunFromTask.setStatus(SCHEDULED);
                    addTaskToQueue(rerunFromTask);
                }
            }
            executionDAOFacade.updateTask(rerunFromTask);
            decide(workflow.getWorkflowId());
            return true;
        }
        return false;
    }

    /**
     * 为 DO_WHILE 循环任务调度下一次迭代：
     * 只调度第一次循环任务，后续由 DeciderService 在任务完成后接管。
     */
    public void scheduleNextIteration(TaskModel loopTask, WorkflowModel workflow) {
        // 只调度第一次循环任务，后续由 DeciderService 在任务完成后处理
        List<TaskModel> scheduledLoopOverTasks =
                deciderService.getTasksToBeScheduled(
                        workflow,
                        loopTask.getWorkflowTask().getLoopOver().get(0),
                        loopTask.getRetryCount(),
                        null);
        setTaskDomains(scheduledLoopOverTasks, workflow);
        scheduledLoopOverTasks.forEach(
                t -> {
                    t.setReferenceTaskName(
                            TaskUtils.appendIteration(
                                    t.getReferenceTaskName(), loopTask.getIteration()));
                    t.setIteration(loopTask.getIteration());
                });
        scheduleTask(workflow, scheduledLoopOverTasks);
        workflow.getTasks().addAll(scheduledLoopOverTasks);
    }

    /** 获取任务定义；找不到则抛 TerminateWorkflowException */
    public TaskDef getTaskDefinition(TaskModel task) {
        return task.getTaskDefinition()
                .orElseGet(
                        () ->
                                Optional.ofNullable(
                                                metadataDAO.getTaskDef(
                                                        task.getWorkflowTask().getName()))
                                        .orElseThrow(
                                                () -> {
                                                    String reason =
                                                            String.format(
                                                                    "Invalid task specified. Cannot find task by name %s in the task definitions",
                                                                    task.getWorkflowTask()
                                                                            .getName());
                                                    return new TerminateWorkflowException(reason);
                                                }));
    }

    /** 更新父工作流中的子工作流任务（同步执行结果） */
    @VisibleForTesting
    void updateParentWorkflowTask(WorkflowModel subWorkflow) {
        TaskModel subWorkflowTask =
                executionDAOFacade.getTaskModel(subWorkflow.getParentWorkflowTaskId());
        executeSubworkflowTaskAndSyncData(subWorkflow, subWorkflowTask);
        executionDAOFacade.updateTask(subWorkflowTask);
    }

    /** 执行 SUB_WORKFLOW 系统任务，把子工作流结果同步到父任务 */
    private void executeSubworkflowTaskAndSyncData(
            WorkflowModel subWorkflow, TaskModel subWorkflowTask) {
        WorkflowSystemTask subWorkflowSystemTask =
                systemTaskRegistry.get(TaskType.TASK_TYPE_SUB_WORKFLOW);
        subWorkflowSystemTask.execute(subWorkflow, subWorkflowTask, this);
    }

    /**
     * 把工作流 id 以更高优先级推入 decider 队列，加速评估。
     * 若队列中已有该消息，则用 postpone 提升优先级。
     *
     * @param workflowId 需要加速评估的工作流
     */
    private void expediteLazyWorkflowEvaluation(String workflowId) {
        if (queueDAO.containsMessage(DECIDER_QUEUE, workflowId)) {
            queueDAO.postpone(DECIDER_QUEUE, workflowId, EXPEDITED_PRIORITY, 0);
        } else {
            queueDAO.push(DECIDER_QUEUE, workflowId, EXPEDITED_PRIORITY, 0);
        }

        LOGGER.info("Pushed workflow {} to {} for expedited evaluation", workflowId, DECIDER_QUEUE);
    }
}