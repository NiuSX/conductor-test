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
package com.netflix.conductor.core.execution.tasks;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.netflix.conductor.common.utils.TaskUtils;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_JOIN;

/**
 * JOIN 系统任务：用于 FORK_JOIN 的汇合点。
 *
 * 它等待所有"被 fork 的分支任务"都到达终态（joinOn 列表中的任务），
 * 然后根据分支结果决定自己是 COMPLETED / FAILED / COMPLETED_WITH_ERRORS。
 *
 * 它是异步系统任务：由引擎在调度周期中反复调用 execute() 检查是否所有分支都完成。
 */
@Component(TASK_TYPE_JOIN)
public class Join extends WorkflowSystemTask {

    public Join() {
        super(TASK_TYPE_JOIN);
    }

    /**
     * 检查所有 joinOn 分支任务是否都已终态：
     * - 若有非可选分支失败 → 本任务 FAILED，并把失败原因汇总
     * - 若所有分支完成但有可选分支 COMPLETED_WITH_ERRORS → 本任务 COMPLETED_WITH_ERRORS
     * - 若所有分支成功完成 → 本任务 COMPLETED
     * - 否则（还有分支未终态）→ 返回 false，继续等待
     *
     * 同时把各分支的输出合并到本任务的输出中，供下游任务引用。
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean execute(
            WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {

        boolean allDone = true;
        boolean hasFailures = false;
        StringBuilder failureReason = new StringBuilder();
        StringBuilder optionalTaskFailures = new StringBuilder();
        // joinOn：需要等待的分支任务引用名列表
        List<String> joinOn = (List<String>) task.getInputData().get("joinOn");
        if (task.isLoopOverTask()) {
            // 若 JOIN 处于循环任务中，需要等待特定迭代的分支完成，给引用名加上迭代后缀
            joinOn =
                    joinOn.stream()
                            .map(name -> TaskUtils.appendIteration(name, task.getIteration()))
                            .collect(Collectors.toList());
        }
        for (String joinOnRef : joinOn) {
            TaskModel forkedTask = workflow.getTaskByRefName(joinOnRef);
            if (forkedTask == null) {
                // 分支任务还没被调度
                allDone = false;
                break;
            }
            TaskModel.Status taskStatus = forkedTask.getStatus();
            // 非成功且不是可选任务 → 视为失败
            hasFailures = !taskStatus.isSuccessful() && !forkedTask.getWorkflowTask().isOptional();
            if (hasFailures) {
                failureReason.append(forkedTask.getReasonForIncompletion()).append(" ");
            }
            // 合并分支输出到 JOIN 任务输出（仅当非空）
            if (!forkedTask.getOutputData().isEmpty()) {
                task.addOutput(joinOnRef, forkedTask.getOutputData());
            }
            if (!taskStatus.isTerminal()) {
                // 还有分支未终态
                allDone = false;
            }
            if (hasFailures) {
                // 一旦有失败就跳出，快速失败
                break;
            }

            // 收集可选任务的失败（COMPLETED_WITH_ERRORS）
            if (forkedTask.getWorkflowTask().isOptional()
                    && taskStatus == TaskModel.Status.COMPLETED_WITH_ERRORS) {
                optionalTaskFailures
                        .append(
                                String.format(
                                        "%s/%s",
                                        forkedTask.getTaskDefName(), forkedTask.getTaskId()))
                        .append(" ");
            }
        }
        // 所有分支终态，或出现失败，或存在可选任务失败 → 确定 JOIN 任务状态
        if (allDone || hasFailures || optionalTaskFailures.length() > 0) {
            if (hasFailures) {
                // 有非可选分支失败 → JOIN 失败
                task.setReasonForIncompletion(failureReason.toString());
                task.setStatus(TaskModel.Status.FAILED);
            } else if (optionalTaskFailures.length() > 0) {
                // 只有可选分支失败 → JOIN 完成但有错误
                task.setStatus(TaskModel.Status.COMPLETED_WITH_ERRORS);
                optionalTaskFailures.append("completed with errors");
                task.setReasonForIncompletion(optionalTaskFailures.toString());
            } else {
                // 全部分支成功 → JOIN 完成
                task.setStatus(TaskModel.Status.COMPLETED);
            }
            return true;
        }
        return false;
    }

    /**
     * 计算 JOIN 任务的评估偏移量（指数退避）。
     * 轮询次数越多，下次检查的间隔越长，避免频繁检查未完成的分支。
     *
     * 逻辑：
     * - index = pollCount - 1（首次为 0）
     * - index == 0 → 立即评估（偏移 0）
     * - 否则 → min(2^index, defaultOffset)
     */
    @Override
    public Optional<Long> getEvaluationOffset(TaskModel taskModel, long defaultOffset) {
        int index = taskModel.getPollCount() > 0 ? taskModel.getPollCount() - 1 : 0;
        if (index == 0) {
            return Optional.of(0L);
        }
        return Optional.of(Math.min((long) Math.pow(2, index), defaultOffset));
    }

    /** 异步系统任务：由引擎在调度周期中反复调用 execute() 检查分支状态 */
    public boolean isAsync() {
        return true;
    }
}