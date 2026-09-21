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

import org.springframework.stereotype.Component;

import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_WAIT;
import static com.netflix.conductor.model.TaskModel.Status.*;

/**
 * WAIT 任务：Conductor 的内置系统任务，用于让工作流在某个时间点之前保持等待。
 * 它是一个“异步任务”，不会被立即完成，而是由引擎在后续调度周期中反复调用 execute()
 * 来检查是否已到达等待截止时间。
 */
@Component(TASK_TYPE_WAIT)
public class Wait extends WorkflowSystemTask {

    /** 输入参数名：等待时长（通常用于计算 waitTimeout） */
    public static final String DURATION_INPUT = "duration";
    /** 输入参数名：等待截止时间点 */
    public static final String UNTIL_INPUT = "until";

    /** 构造器：注册任务类型为 WAIT */
    public Wait() {
        super(TASK_TYPE_WAIT);
    }

    /**
     * 取消任务：直接把状态置为 CANCELED。
     * 等待任务没有副作用需要清理，所以实现很简单。
     */
    @Override
    public void cancel(WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        task.setStatus(TaskModel.Status.CANCELED);
    }

    /**
     * 执行/检查任务：
     * 该方法会被引擎反复调用（因为 isAsync() 返回 true），用来判断等待是否结束。
     *
     * 逻辑：
     * 1. 若 waitTimeout 为 0，说明没有设置等待截止时间，直接返回 false（继续等待/不做处理）。
     * 2. 若当前时间已超过 waitTimeout，说明等待结束，把任务置为 COMPLETED，返回 true（表示任务状态已变更）。
     * 3. 否则还没到时间，返回 false（本次不改变状态，下次再检查）。
     */
    @Override
    public boolean execute(
            WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        // 取出该任务的等待截止时间（毫秒时间戳）
        long timeOut = task.getWaitTimeout();
        // 未设置截止时间：不做处理，返回 false
        if (timeOut == 0) {
            return false;
        }
        // 当前时间已超过截止时间：等待结束，标记为完成
        if (System.currentTimeMillis() > timeOut) {
            task.setStatus(COMPLETED);
            return true;
        }

        // 还没到截止时间，继续等待
        return false;
    }

    /** 标识该任务为异步任务：由引擎在后续调度周期反复调用 execute() 检查 */
    public boolean isAsync() {
        return true;
    }
}