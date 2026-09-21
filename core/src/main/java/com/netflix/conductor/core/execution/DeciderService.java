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

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.annotations.VisibleForTesting;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.utils.ExternalPayloadStorage.Operation;
import com.netflix.conductor.common.utils.ExternalPayloadStorage.PayloadType;
import com.netflix.conductor.common.utils.TaskUtils;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.core.execution.mapper.TaskMapper;
import com.netflix.conductor.core.execution.mapper.TaskMapperContext;
import com.netflix.conductor.core.execution.tasks.SystemTaskRegistry;
import com.netflix.conductor.core.utils.ExternalPayloadStorageUtils;
import com.netflix.conductor.core.utils.IDGenerator;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TERMINATE;
import static com.netflix.conductor.common.metadata.tasks.TaskType.USER_DEFINED;
import static com.netflix.conductor.model.TaskModel.Status.*;

/**
 * Decider（决策器）：通过检查工作流当前状态 + 蓝图（定义），评估工作流接下来该做什么。
 * 评估结果有三种：调度更多任务、完成/失败工作流、或什么都不做。
 *
 * 它是 Conductor "持久化执行"的核心：
 * - 每次 decide 都是幂等的、可重入的
 * - 基于已持久化的任务状态推导下一步
 * - 不依赖内存中的临时状态，从而支持故障恢复
 */
@Service
@Trace
public class DeciderService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeciderService.class);

    private final IDGenerator idGenerator;
    private final ParametersUtils parametersUtils;
    private final ExternalPayloadStorageUtils externalPayloadStorageUtils;
    private final MetadataDAO metadataDAO;
    private final SystemTaskRegistry systemTaskRegistry;
    /** 任务处于 pending 状态超过该阈值（分钟）会打警告日志 */
    private final long taskPendingTimeThresholdMins;

    /** 任务类型 → TaskMapper 的映射，用于把 WorkflowTask 映射成 TaskModel */
    private final Map<String, TaskMapper> taskMappers;

    public DeciderService(
            IDGenerator idGenerator,
            ParametersUtils parametersUtils,
            MetadataDAO metadataDAO,
            ExternalPayloadStorageUtils externalPayloadStorageUtils,
            SystemTaskRegistry systemTaskRegistry,
            @Qualifier("taskMappersByTaskType") Map<String, TaskMapper> taskMappers,
            @Value("${conductor.app.taskPendingTimeThreshold:60m}")
            Duration taskPendingTimeThreshold) {
        this.idGenerator = idGenerator;
        this.metadataDAO = metadataDAO;
        this.parametersUtils = parametersUtils;
        this.taskMappers = taskMappers;
        this.externalPayloadStorageUtils = externalPayloadStorageUtils;
        this.taskPendingTimeThresholdMins = taskPendingTimeThreshold.toMinutes();
        this.systemTaskRegistry = systemTaskRegistry;
    }

    /**
     * decide 入口：判断是新工作流还是已有工作流。
     * - 新工作流（无未处理任务）：调用 startWorkflow 调度第一个任务
     * - 已有工作流：直接进入 decide(workflow, tasksToBeScheduled)
     */
    public DeciderOutcome decide(WorkflowModel workflow) throws TerminateWorkflowException {

        // 新工作流的任务列表为空
        final List<TaskModel> tasks = workflow.getTasks();
        // 过滤出"未执行、未被跳过"的任务
        List<TaskModel> unprocessedTasks =
                tasks.stream()
                        .filter(t -> !t.getStatus().equals(SKIPPED) && !t.isExecuted())
                        .collect(Collectors.toList());

        List<TaskModel> tasksToBeScheduled = new LinkedList<>();
        if (unprocessedTasks.isEmpty()) {
            // 新工作流走这里：调度起始任务
            tasksToBeScheduled = startWorkflow(workflow);
            if (tasksToBeScheduled == null) {
                tasksToBeScheduled = new LinkedList<>();
            }
        }
        return decide(workflow, tasksToBeScheduled);
    }

    /**
     * decide 的核心实现：基于当前任务状态推导下一步。
     *
     * 流程：
     * 1. 终态/暂停工作流直接返回
     * 2. 检查工作流超时
     * 3. 收集 pending 任务、已执行任务名、TERMINATE 任务
     * 4. 对每个 pending 任务：检查超时/轮询超时、决定是否重试
     * 5. 对终态任务：标记 executed，计算下一个任务
     * 6. 汇总待调度任务，判断工作流是否完成
     */
    private DeciderOutcome decide(final WorkflowModel workflow, List<TaskModel> preScheduledTasks)
            throws TerminateWorkflowException {

        DeciderOutcome outcome = new DeciderOutcome();

        // 终态工作流不可再评估
        if (workflow.getStatus().isTerminal()) {
            LOGGER.debug(
                    "Workflow {} is already finished. Reason: {}",
                    workflow,
                    workflow.getReasonForIncompletion());
            return outcome;
        }

        // 检查工作流级超时
        checkWorkflowTimeout(workflow);

        // 暂停的工作流不推进
        if (workflow.getStatus().equals(WorkflowModel.Status.PAUSED)) {
            LOGGER.debug("Workflow " + workflow.getWorkflowId() + " is paused");
            return outcome;
        }

        List<TaskModel> pendingTasks = new ArrayList<>();
        Set<String> executedTaskRefNames = new HashSet<>();
        boolean hasSuccessfulTerminateTask = false;
        for (TaskModel task : workflow.getTasks()) {

            // 收集"未重试、未执行、未跳过"的 pending 任务
            if (!task.isRetried() && !task.getStatus().equals(SKIPPED) && !task.isExecuted()) {
                pendingTasks.add(task);
            }

            // 收集已执行任务的引用名
            if (task.isExecuted()) {
                executedTaskRefNames.add(task.getReferenceTaskName());
            }

            // 记录成功的 TERMINATE 任务（用于结束工作流）
            if (TERMINATE.name().equals(task.getTaskType())
                    && task.getStatus().isTerminal()
                    && task.getStatus().isSuccessful()) {
                hasSuccessfulTerminateTask = true;
                outcome.terminateTask = task;
            }
        }

        Map<String, TaskModel> tasksToBeScheduled = new LinkedHashMap<>();

        // 预先调度的任务先放入 map
        preScheduledTasks.forEach(
                preScheduledTask -> {
                    tasksToBeScheduled.put(
                            preScheduledTask.getReferenceTaskName(), preScheduledTask);
                });

        // 新工作流不会进入这个循环
        for (TaskModel pendingTask : pendingTasks) {

            // 非终态的系统任务：加入待调度，并从已执行集合移除
            if (systemTaskRegistry.isSystemTask(pendingTask.getTaskType())
                    && !pendingTask.getStatus().isTerminal()) {
                tasksToBeScheduled.putIfAbsent(pendingTask.getReferenceTaskName(), pendingTask);
                executedTaskRefNames.remove(pendingTask.getReferenceTaskName());
            }

            Optional<TaskDef> taskDefinition = pendingTask.getTaskDefinition();
            if (taskDefinition.isEmpty()) {
                taskDefinition =
                        Optional.ofNullable(
                                        workflow.getWorkflowDefinition()
                                                .getTaskByRefName(
                                                        pendingTask.getReferenceTaskName()))
                                .map(WorkflowTask::getTaskDefinition);
            }

            if (taskDefinition.isPresent()) {
                checkTaskTimeout(taskDefinition.get(), pendingTask);
                checkTaskPollTimeout(taskDefinition.get(), pendingTask);
                // 若任务超过 responseTimeoutSeconds 未更新，标记为 TIMED_OUT
                if (isResponseTimedOut(taskDefinition.get(), pendingTask)) {
                    timeoutTask(taskDefinition.get(), pendingTask);
                }
            }

            // 任务未成功：尝试重试
            if (!pendingTask.getStatus().isSuccessful()) {
                WorkflowTask workflowTask = pendingTask.getWorkflowTask();
                if (workflowTask == null) {
                    workflowTask =
                            workflow.getWorkflowDefinition()
                                    .getTaskByRefName(pendingTask.getReferenceTaskName());
                }

                Optional<TaskModel> retryTask =
                        retry(taskDefinition.orElse(null), workflowTask, pendingTask, workflow);
                if (retryTask.isPresent()) {
                    // 有重试任务：加入待调度
                    tasksToBeScheduled.put(retryTask.get().getReferenceTaskName(), retryTask.get());
                    executedTaskRefNames.remove(retryTask.get().getReferenceTaskName());
                    outcome.tasksToBeUpdated.add(pendingTask);
                } else {
                    // 无可重试：标记为 COMPLETED_WITH_ERRORS
                    pendingTask.setStatus(COMPLETED_WITH_ERRORS);
                }
            }

            // 终态且未执行的任务：标记 executed，计算下一个任务
            if (!pendingTask.isExecuted()
                    && !pendingTask.isRetried()
                    && pendingTask.getStatus().isTerminal()) {
                pendingTask.setExecuted(true);
                List<TaskModel> nextTasks = getNextTask(workflow, pendingTask);
                // 循环任务：过滤出本次迭代还没跑的下游任务
                if (pendingTask.isLoopOverTask()
                        && !TaskType.DO_WHILE.name().equals(pendingTask.getTaskType())
                        && !nextTasks.isEmpty()) {
                    nextTasks = filterNextLoopOverTasks(nextTasks, pendingTask, workflow);
                }
                nextTasks.forEach(
                        nextTask ->
                                tasksToBeScheduled.putIfAbsent(
                                        nextTask.getReferenceTaskName(), nextTask));
                outcome.tasksToBeUpdated.add(pendingTask);
                LOGGER.debug(
                        "Scheduling Tasks from {}, next = {} for workflowId: {}",
                        pendingTask.getTaskDefName(),
                        nextTasks.stream()
                                .map(TaskModel::getTaskDefName)
                                .collect(Collectors.toList()),
                        workflow.getWorkflowId());
            }
        }

        // 过滤掉已执行的任务，得到最终待调度列表
        List<TaskModel> unScheduledTasks =
                tasksToBeScheduled.values().stream()
                        .filter(task -> !executedTaskRefNames.contains(task.getReferenceTaskName()))
                        .collect(Collectors.toList());
        if (!unScheduledTasks.isEmpty()) {
            LOGGER.debug(
                    "Scheduling Tasks: {} for workflow: {}",
                    unScheduledTasks.stream()
                            .map(TaskModel::getTaskDefName)
                            .collect(Collectors.toList()),
                    workflow.getWorkflowId());
            outcome.tasksToBeScheduled.addAll(unScheduledTasks);
        }

        // 判断工作流是否完成
        if (hasSuccessfulTerminateTask
                || (outcome.tasksToBeScheduled.isEmpty() && checkForWorkflowCompletion(workflow))) {
            LOGGER.debug("Marking workflow: {} as complete.", workflow);
            outcome.isComplete = true;
        }

        return outcome;
    }

    /**
     * 过滤循环任务的下一次迭代任务：跳过工作流中已存在（进行中/终态）的任务。
     */
    @VisibleForTesting
    List<TaskModel> filterNextLoopOverTasks(
            List<TaskModel> tasks, TaskModel pendingTask, WorkflowModel workflow) {

        // 更新任务引用名和迭代号
        tasks.forEach(
                nextTask -> {
                    nextTask.setReferenceTaskName(
                            TaskUtils.appendIteration(
                                    nextTask.getReferenceTaskName(), pendingTask.getIteration()));
                    nextTask.setIteration(pendingTask.getIteration());
                });

        List<String> tasksInWorkflow =
                workflow.getTasks().stream()
                        .filter(
                                runningTask ->
                                        runningTask.getStatus().equals(TaskModel.Status.IN_PROGRESS)
                                                || runningTask.getStatus().isTerminal())
                        .map(TaskModel::getReferenceTaskName)
                        .collect(Collectors.toList());

        return tasks.stream()
                .filter(
                        runningTask ->
                                !tasksInWorkflow.contains(runningTask.getReferenceTaskName()))
                .collect(Collectors.toList());
    }

    /**
     * 启动新工作流：调度第一个非跳过任务。
     * 若是 re-run 场景，则从指定的起始任务开始。
     */
    private List<TaskModel> startWorkflow(WorkflowModel workflow)
            throws TerminateWorkflowException {
        final WorkflowDef workflowDef = workflow.getWorkflowDefinition();

        LOGGER.debug("Starting workflow: {}", workflow);

        List<TaskModel> tasks = workflow.getTasks();
        // 判断是新工作流还是 re-run
        if (workflow.getReRunFromWorkflowId() == null || tasks.isEmpty()) {

            if (workflowDef.getTasks().isEmpty()) {
                throw new TerminateWorkflowException(
                        "No tasks found to be executed", WorkflowModel.Status.COMPLETED);
            }

            // 调度第一个任务
            WorkflowTask taskToSchedule = workflowDef.getTasks().get(0);
            // 跳过被标记为 skipped 的任务
            while (isTaskSkipped(taskToSchedule, workflow)) {
                taskToSchedule = workflowDef.getNextTask(taskToSchedule.getTaskReferenceName());
            }

            return getTasksToBeScheduled(workflow, taskToSchedule, 0);
        }

        // re-run 场景：找到起始任务并重置为 SCHEDULED
        TaskModel rerunFromTask =
                tasks.stream()
                        .findFirst()
                        .map(
                                task -> {
                                    task.setStatus(SCHEDULED);
                                    task.setRetried(true);
                                    task.setRetryCount(0);
                                    return task;
                                })
                        .orElseThrow(
                                () -> {
                                    String reason =
                                            String.format(
                                                    "The workflow %s is marked for re-run from %s but could not find the starting task",
                                                    workflow.getWorkflowId(),
                                                    workflow.getReRunFromWorkflowId());
                                    return new TerminateWorkflowException(reason);
                                });

        return Collections.singletonList(rerunFromTask);
    }

    /**
     * 更新工作流输出。
     *
     * @param workflow 工作流实例
     * @param task 若不为 null，则用该任务的输出作为工作流输出（当定义未指定 outputParameters 时）；
     *             若为 null，则用最后一个任务的输出
     */
    void updateWorkflowOutput(final WorkflowModel workflow, TaskModel task) {
        List<TaskModel> allTasks = workflow.getTasks();
        if (allTasks.isEmpty()) {
            return;
        }

        Map<String, Object> output = new HashMap<>();
        Optional<TaskModel> optionalTask =
                allTasks.stream()
                        .filter(
                                t ->
                                        TaskType.TERMINATE.name().equals(t.getTaskType())
                                                && t.getStatus().isTerminal()
                                                && t.getStatus().isSuccessful())
                        .findFirst();
        if (optionalTask.isPresent()) {
            // 有成功的 TERMINATE 任务：用它的输出
            TaskModel terminateTask = optionalTask.get();
            if (StringUtils.isNotBlank(terminateTask.getExternalOutputPayloadStoragePath())) {
                output =
                        externalPayloadStorageUtils.downloadPayload(
                                terminateTask.getExternalOutputPayloadStoragePath());
                Monitors.recordExternalPayloadStorageUsage(
                        terminateTask.getTaskDefName(),
                        Operation.READ.toString(),
                        PayloadType.TASK_OUTPUT.toString());
            } else if (!terminateTask.getOutputData().isEmpty()) {
                output = terminateTask.getOutputData();
            }
        } else {
            // 无 TERMINATE：用定义中的 outputParameters，或最后一个任务的输出
            TaskModel last = Optional.ofNullable(task).orElse(allTasks.get(allTasks.size() - 1));
            WorkflowDef workflowDef = workflow.getWorkflowDefinition();
            if (workflowDef.getOutputParameters() != null
                    && !workflowDef.getOutputParameters().isEmpty()) {
                output =
                        parametersUtils.getTaskInput(
                                workflowDef.getOutputParameters(), workflow, null, null);
            } else if (StringUtils.isNotBlank(last.getExternalOutputPayloadStoragePath())) {
                output =
                        externalPayloadStorageUtils.downloadPayload(
                                last.getExternalOutputPayloadStoragePath());
                Monitors.recordExternalPayloadStorageUsage(
                        last.getTaskDefName(),
                        Operation.READ.toString(),
                        PayloadType.TASK_OUTPUT.toString());
            } else {
                output = last.getOutputData();
            }
        }
        workflow.setOutput(output);
    }

    /**
     * 判断工作流是否已完成：
     * - 所有任务都是终态
     * - 没有失败任务
     * - 定义中所有任务都成功完成
     * - 没有待调度的下游任务
     */
    public boolean checkForWorkflowCompletion(final WorkflowModel workflow)
            throws TerminateWorkflowException {

        Map<String, TaskModel.Status> taskStatusMap = new HashMap<>();
        List<TaskModel> nonExecutedTasks = new ArrayList<>();
        for (TaskModel task : workflow.getTasks()) {
            taskStatusMap.put(task.getReferenceTaskName(), task.getStatus());
            if (!task.getStatus().isTerminal()) {
                return false;
            }

            // 有成功的 TERMINATE 任务则视为完成
            if (TERMINATE.name().equals(task.getTaskType())
                    && task.getStatus().isTerminal()
                    && task.getStatus().isSuccessful()) {
                return true;
            }
            if (!task.isRetried() || !task.isExecuted()) {
                nonExecutedTasks.add(task);
            }
        }

        if (taskStatusMap.isEmpty()) {
            return false;
        }

        List<WorkflowTask> workflowTasks = workflow.getWorkflowDefinition().getTasks();

        // 定义中每个任务都必须是终态且成功
        for (WorkflowTask wftask : workflowTasks) {
            TaskModel.Status status = taskStatusMap.get(wftask.getTaskReferenceName());
            if (status == null || !status.isTerminal()) {
                return false;
            }
            if (!status.isSuccessful()) {
                return false;
            }
        }

        // 没有待调度的下游任务
        boolean noPendingSchedule =
                nonExecutedTasks.stream()
                        .parallel()
                        .noneMatch(
                                wftask -> {
                                    String next = getNextTasksToBeScheduled(workflow, wftask);
                                    return next != null && !taskStatusMap.containsKey(next);
                                });

        return noPendingSchedule;
    }

    /** 获取某个任务完成后的下一个任务（映射为 TaskModel 列表） */
    List<TaskModel> getNextTask(WorkflowModel workflow, TaskModel task) {
        final WorkflowDef workflowDef = workflow.getWorkflowDefinition();

        // DECISION / SWITCH 任务若已处理子分支，则不再返回下游
        if (systemTaskRegistry.isSystemTask(task.getTaskType())
                && (TaskType.TASK_TYPE_DECISION.equals(task.getTaskType())
                || TaskType.TASK_TYPE_SWITCH.equals(task.getTaskType()))) {
            if (task.getInputData().get("hasChildren") != null) {
                return Collections.emptyList();
            }
        }

        // 循环任务去掉迭代后缀再查下游
        String taskReferenceName =
                task.isLoopOverTask()
                        ? TaskUtils.removeIterationFromTaskRefName(task.getReferenceTaskName())
                        : task.getReferenceTaskName();
        WorkflowTask taskToSchedule = workflowDef.getNextTask(taskReferenceName);
        while (isTaskSkipped(taskToSchedule, workflow)) {
            taskToSchedule = workflowDef.getNextTask(taskToSchedule.getTaskReferenceName());
        }
        if (taskToSchedule != null && TaskType.DO_WHILE.name().equals(taskToSchedule.getType())) {
            // DO_WHILE 已存在则不再重复调度
            String nextTaskReferenceName = taskToSchedule.getTaskReferenceName();
            if (workflow.getTasks().stream()
                    .anyMatch(
                            runningTask ->
                                    runningTask
                                            .getReferenceTaskName()
                                            .equals(nextTaskReferenceName))) {
                return Collections.emptyList();
            }
        }
        if (taskToSchedule != null) {
            return getTasksToBeScheduled(workflow, taskToSchedule, 0);
        }

        return Collections.emptyList();
    }

    /** 只返回下一个待调度任务的引用名（用于完成度检查） */
    private String getNextTasksToBeScheduled(WorkflowModel workflow, TaskModel task) {
        final WorkflowDef def = workflow.getWorkflowDefinition();

        String taskReferenceName = task.getReferenceTaskName();
        WorkflowTask taskToSchedule = def.getNextTask(taskReferenceName);
        while (isTaskSkipped(taskToSchedule, workflow)) {
            taskToSchedule = def.getNextTask(taskToSchedule.getTaskReferenceName());
        }
        return taskToSchedule == null ? null : taskToSchedule.getTaskReferenceName();
    }

    /**
     * 重试逻辑：
     * - 若不满足重试条件（非可重试状态、内置任务、已达最大重试次数）：
     *   - 可选任务返回 empty
     *   - 否则根据状态抛出 TerminateWorkflowException（FAILED / TERMINATED / TIMED_OUT）
     * - 满足条件：按重试策略计算延迟，生成新的 SCHEDULED 任务
     *
     * @return 重试任务（若有）
     */
    @VisibleForTesting
    Optional<TaskModel> retry(
            TaskDef taskDefinition,
            WorkflowTask workflowTask,
            TaskModel task,
            WorkflowModel workflow)
            throws TerminateWorkflowException {

        int retryCount = task.getRetryCount();

        if (taskDefinition == null) {
            taskDefinition = metadataDAO.getTaskDef(task.getTaskDefName());
        }

        final int expectedRetryCount =
                taskDefinition == null
                        ? 0
                        : Optional.ofNullable(workflowTask)
                        .map(WorkflowTask::getRetryCount)
                        .orElse(taskDefinition.getRetryCount());
        // 不满足重试条件
        if (!task.getStatus().isRetriable()
                || TaskType.isBuiltIn(task.getTaskType())
                || expectedRetryCount <= retryCount) {
            if (workflowTask != null && workflowTask.isOptional()) {
                return Optional.empty();
            }
            WorkflowModel.Status status;
            switch (task.getStatus()) {
                case CANCELED:
                    status = WorkflowModel.Status.TERMINATED;
                    break;
                case TIMED_OUT:
                    status = WorkflowModel.Status.TIMED_OUT;
                    break;
                default:
                    status = WorkflowModel.Status.FAILED;
                    break;
            }
            updateWorkflowOutput(workflow, task);
            final String errMsg =
                    String.format(
                            "Task %s failed with status: %s and reason: '%s'",
                            task.getTaskId(), status, task.getReasonForIncompletion());
            throw new TerminateWorkflowException(errMsg, status, task);
        }

        // 计算重试延迟
        int startDelay = taskDefinition.getRetryDelaySeconds();
        switch (taskDefinition.getRetryLogic()) {
            case FIXED:
                startDelay = taskDefinition.getRetryDelaySeconds();
                break;
            case LINEAR_BACKOFF:
                int linearRetryDelaySeconds =
                        taskDefinition.getRetryDelaySeconds()
                                * taskDefinition.getBackoffScaleFactor()
                                * (task.getRetryCount() + 1);
                startDelay =
                        linearRetryDelaySeconds < 0 ? Integer.MAX_VALUE : linearRetryDelaySeconds;
                break;
            case EXPONENTIAL_BACKOFF:
                int exponentialRetryDelaySeconds =
                        taskDefinition.getRetryDelaySeconds()
                                * (int) Math.pow(2, task.getRetryCount());
                startDelay =
                        exponentialRetryDelaySeconds < 0
                                ? Integer.MAX_VALUE
                                : exponentialRetryDelaySeconds;
                break;
        }

        task.setRetried(true);

        // 构造重试任务：新 taskId、retryCount+1、SCHEDULED
        TaskModel rescheduled = task.copy();
        rescheduled.setStartDelayInSeconds(startDelay);
        rescheduled.setCallbackAfterSeconds(startDelay);
        rescheduled.setRetryCount(task.getRetryCount() + 1);
        rescheduled.setRetried(false);
        rescheduled.setTaskId(idGenerator.generate());
        rescheduled.setRetriedTaskId(task.getTaskId());
        rescheduled.setStatus(SCHEDULED);
        rescheduled.setPollCount(0);
        rescheduled.setInputData(new HashMap<>(task.getInputData()));
        rescheduled.setReasonForIncompletion(null);
        rescheduled.setSubWorkflowId(null);
        rescheduled.setSeq(0);
        rescheduled.setScheduledTime(0);
        rescheduled.setStartTime(0);
        rescheduled.setEndTime(0);
        rescheduled.setWorkerId(null);

        if (StringUtils.isNotBlank(task.getExternalInputPayloadStoragePath())) {
            rescheduled.setExternalInputPayloadStoragePath(
                    task.getExternalInputPayloadStoragePath());
        } else {
            rescheduled.addInput(task.getInputData());
        }
        // schema version > 1 时需要重新计算输入参数
        if (workflowTask != null && workflow.getWorkflowDefinition().getSchemaVersion() > 1) {
            Map<String, Object> taskInput =
                    parametersUtils.getTaskInputV2(
                            workflowTask.getInputParameters(),
                            workflow,
                            rescheduled.getTaskId(),
                            taskDefinition);
            rescheduled.addInput(taskInput);
        }
        return Optional.of(rescheduled);
    }

    /** 检查工作流级超时，按 timeoutPolicy 处理（ALERT_ONLY / TIME_OUT_WF） */
    @VisibleForTesting
    void checkWorkflowTimeout(WorkflowModel workflow) {
        WorkflowDef workflowDef = workflow.getWorkflowDefinition();
        if (workflowDef == null) {
            LOGGER.warn("Missing workflow definition : {}", workflow.getWorkflowId());
            return;
        }
        if (workflow.getStatus().isTerminal() || workflowDef.getTimeoutSeconds() <= 0) {
            return;
        }

        long timeout = 1000L * workflowDef.getTimeoutSeconds();
        long now = System.currentTimeMillis();
        long elapsedTime =
                workflow.getLastRetriedTime() > 0
                        ? now - workflow.getLastRetriedTime()
                        : now - workflow.getCreateTime();

        if (elapsedTime < timeout) {
            return;
        }

        String reason =
                String.format(
                        "Workflow timed out after %d seconds. Timeout configured as %d seconds. "
                                + "Timeout policy configured to %s",
                        elapsedTime / 1000L,
                        workflowDef.getTimeoutSeconds(),
                        workflowDef.getTimeoutPolicy().name());

        switch (workflowDef.getTimeoutPolicy()) {
            case ALERT_ONLY:
                // 仅告警，不终止
                LOGGER.info("{} {}", workflow.getWorkflowId(), reason);
                Monitors.recordWorkflowTermination(
                        workflow.getWorkflowName(),
                        WorkflowModel.Status.TIMED_OUT,
                        workflow.getOwnerApp());
                return;
            case TIME_OUT_WF:
                // 超时则终止工作流
                throw new TerminateWorkflowException(reason, WorkflowModel.Status.TIMED_OUT);
        }
    }

    /** 检查任务级超时（timeoutSeconds），按 timeoutPolicy 处理 */
    @VisibleForTesting
    void checkTaskTimeout(TaskDef taskDef, TaskModel task) {

        if (taskDef == null) {
            LOGGER.warn(
                    "Missing task definition for task:{}/{} in workflow:{}",
                    task.getTaskId(),
                    task.getTaskDefName(),
                    task.getWorkflowInstanceId());
            return;
        }
        if (task.getStatus().isTerminal()
                || taskDef.getTimeoutSeconds() <= 0
                || task.getStartTime() <= 0) {
            return;
        }

        long timeout = 1000L * taskDef.getTimeoutSeconds();
        long now = System.currentTimeMillis();
        long elapsedTime =
                now - (task.getStartTime() + ((long) task.getStartDelayInSeconds() * 1000L));

        if (elapsedTime < timeout) {
            return;
        }

        String reason =
                String.format(
                        "Task timed out after %d seconds. Timeout configured as %d seconds. "
                                + "Timeout policy configured to %s",
                        elapsedTime / 1000L,
                        taskDef.getTimeoutSeconds(),
                        taskDef.getTimeoutPolicy().name());
        timeoutTaskWithTimeoutPolicy(reason, taskDef, task);
    }

    /** 检查任务轮询超时（pollTimeoutSeconds），针对 SCHEDULED 状态的任务 */
    @VisibleForTesting
    void checkTaskPollTimeout(TaskDef taskDef, TaskModel task) {
        if (taskDef == null) {
            LOGGER.warn(
                    "Missing task definition for task:{}/{} in workflow:{}",
                    task.getTaskId(),
                    task.getTaskDefName(),
                    task.getWorkflowInstanceId());
            return;
        }
        if (taskDef.getPollTimeoutSeconds() == null
                || taskDef.getPollTimeoutSeconds() <= 0
                || !task.getStatus().equals(SCHEDULED)) {
            return;
        }

        final long pollTimeout = 1000L * taskDef.getPollTimeoutSeconds();
        final long adjustedPollTimeout = pollTimeout + task.getCallbackAfterSeconds() * 1000L;
        final long now = System.currentTimeMillis();
        final long pollElapsedTime =
                now - (task.getScheduledTime() + ((long) task.getStartDelayInSeconds() * 1000L));

        if (pollElapsedTime < adjustedPollTimeout) {
            return;
        }

        String reason =
                String.format(
                        "Task poll timed out after %d seconds. Poll timeout configured as %d seconds. Timeout policy configured to %s",
                        pollElapsedTime / 1000L,
                        pollTimeout / 1000L,
                        taskDef.getTimeoutPolicy().name());
        timeoutTaskWithTimeoutPolicy(reason, taskDef, task);
    }

    /** 按 timeoutPolicy 处理超时任务：ALERT_ONLY / RETRY / TIME_OUT_WF */
    void timeoutTaskWithTimeoutPolicy(String reason, TaskDef taskDef, TaskModel task) {
        Monitors.recordTaskTimeout(task.getTaskDefName());

        switch (taskDef.getTimeoutPolicy()) {
            case ALERT_ONLY:
                LOGGER.info(reason);
                return;
            case RETRY:
                // 标记超时，后续重试逻辑会处理
                task.setStatus(TIMED_OUT);
                task.setReasonForIncompletion(reason);
                return;
            case TIME_OUT_WF:
                // 超时直接终止工作流
                task.setStatus(TIMED_OUT);
                task.setReasonForIncompletion(reason);
                throw new TerminateWorkflowException(reason, WorkflowModel.Status.TIMED_OUT, task);
        }
    }

    /**
     * 判断任务是否响应超时（responseTimeoutSeconds）。
     * 针对 IN_PROGRESS 的任务，若超过 responseTimeout 未更新则视为超时。
     */
    @VisibleForTesting
    boolean isResponseTimedOut(TaskDef taskDefinition, TaskModel task) {
        if (taskDefinition == null) {
            LOGGER.warn(
                    "missing task type : {}, workflowId= {}",
                    task.getTaskDefName(),
                    task.getWorkflowInstanceId());
            return false;
        }

        if (task.getStatus().isTerminal() || isAyncCompleteSystemTask(task)) {
            return false;
        }

        // 计算 pending 时间，超阈值打警告
        long now = System.currentTimeMillis();
        long callbackTime = 1000L * task.getCallbackAfterSeconds();
        long referenceTime =
                task.getUpdateTime() > 0 ? task.getUpdateTime() : task.getScheduledTime();
        long pendingTime = now - (referenceTime + callbackTime);
        Monitors.recordTaskPendingTime(task.getTaskType(), task.getWorkflowType(), pendingTime);
        long thresholdMS = taskPendingTimeThresholdMins * 60 * 1000;
        if (pendingTime > thresholdMS) {
            LOGGER.warn(
                    "Task: {} of type: {} in workflow: {}/{} is in pending state for longer than {} ms",
                    task.getTaskId(),
                    task.getTaskType(),
                    task.getWorkflowInstanceId(),
                    task.getWorkflowType(),
                    thresholdMS);
        }

        if (!task.getStatus().equals(IN_PROGRESS)
                || taskDefinition.getResponseTimeoutSeconds() == 0) {
            return false;
        }

        long responseTimeout = 1000L * taskDefinition.getResponseTimeoutSeconds();
        long adjustedResponseTimeout = responseTimeout + callbackTime;
        long noResponseTime = now - task.getUpdateTime();

        if (noResponseTime < adjustedResponseTimeout) {
            return false;
        }

        Monitors.recordTaskResponseTimeout(task.getTaskDefName());
        return true;
    }

    /** 把任务标记为响应超时 */
    private void timeoutTask(TaskDef taskDef, TaskModel task) {
        String reason =
                "responseTimeout: "
                        + taskDef.getResponseTimeoutSeconds()
                        + " exceeded for the taskId: "
                        + task.getTaskId()
                        + " with Task Definition: "
                        + task.getTaskDefName();
        LOGGER.debug(reason);
        task.setStatus(TIMED_OUT);
        task.setReasonForIncompletion(reason);
    }

    public List<TaskModel> getTasksToBeScheduled(
            WorkflowModel workflow, WorkflowTask taskToSchedule, int retryCount) {
        return getTasksToBeScheduled(workflow, taskToSchedule, retryCount, null);
    }

    /**
     * 把 WorkflowTask 映射为具体的 TaskModel 列表。
     * 通过 taskMappers 找到对应类型的 TaskMapper，由它决定生成哪些任务。
     * 过滤掉工作流中已存在（进行中/终态）的任务，避免重复调度。
     */
    public List<TaskModel> getTasksToBeScheduled(
            WorkflowModel workflow,
            WorkflowTask taskToSchedule,
            int retryCount,
            String retriedTaskId) {
        Map<String, Object> input =
                parametersUtils.getTaskInput(
                        taskToSchedule.getInputParameters(), workflow, null, null);

        String type = taskToSchedule.getType();

        // 工作流中已存在（进行中/终态）的任务引用名
        List<String> tasksInWorkflow =
                workflow.getTasks().stream()
                        .filter(
                                runningTask ->
                                        runningTask.getStatus().equals(TaskModel.Status.IN_PROGRESS)
                                                || runningTask.getStatus().isTerminal())
                        .map(TaskModel::getReferenceTaskName)
                        .collect(Collectors.toList());

        String taskId = idGenerator.generate();
        TaskMapperContext taskMapperContext =
                TaskMapperContext.newBuilder()
                        .withWorkflowModel(workflow)
                        .withTaskDefinition(taskToSchedule.getTaskDefinition())
                        .withWorkflowTask(taskToSchedule)
                        .withTaskInput(input)
                        .withRetryCount(retryCount)
                        .withRetryTaskId(retriedTaskId)
                        .withTaskId(taskId)
                        .withDeciderService(this)
                        .build();

        // 用对应的 TaskMapper 生成任务；同引用名已存在则不重复调度
        return taskMappers
                .getOrDefault(type, taskMappers.get(USER_DEFINED.name()))
                .getMappedTasks(taskMapperContext)
                .stream()
                .filter(task -> !tasksInWorkflow.contains(task.getReferenceTaskName()))
                .collect(Collectors.toList());
    }

    /** 判断任务是否被标记为跳过 */
    private boolean isTaskSkipped(WorkflowTask taskToSchedule, WorkflowModel workflow) {
        try {
            boolean isTaskSkipped = false;
            if (taskToSchedule != null) {
                TaskModel t = workflow.getTaskByRefName(taskToSchedule.getTaskReferenceName());
                if (t == null) {
                    isTaskSkipped = false;
                } else if (t.getStatus().equals(SKIPPED)) {
                    isTaskSkipped = true;
                }
            }
            return isTaskSkipped;
        } catch (Exception e) {
            throw new TerminateWorkflowException(e.getMessage());
        }
    }

    /** 判断是否为"异步完成"的系统任务 */
    private boolean isAyncCompleteSystemTask(TaskModel task) {
        return systemTaskRegistry.isSystemTask(task.getTaskType())
                && systemTaskRegistry.get(task.getTaskType()).isAsyncComplete(task);
    }

    /**
     * decide 的输出结果：
     * - tasksToBeScheduled：需要新调度的任务
     * - tasksToBeUpdated：需要更新的任务
     * - isComplete：工作流是否已完成
     * - terminateTask：触发结束的 TERMINATE 任务
     */
    public static class DeciderOutcome {

        List<TaskModel> tasksToBeScheduled = new LinkedList<>();
        List<TaskModel> tasksToBeUpdated = new LinkedList<>();
        boolean isComplete;
        TaskModel terminateTask;

        private DeciderOutcome() {}
    }
}