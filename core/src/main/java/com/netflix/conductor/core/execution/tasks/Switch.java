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

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_SWITCH;

/**
 * SWITCH 任务：用于工作流中的条件分支。
 * 它是已废弃的 Decision 任务的替代品。
 *
 * 说明：分支的实际决策逻辑（即根据条件选择哪条路径）并不在这个类里完成，
 * 而是在引擎解析任务定义、计算分支输出时处理。这里的 execute() 只是把
 * 任务标记为 COMPLETED，让工作流继续往下走。
 */
@Component(TASK_TYPE_SWITCH)
public class Switch extends WorkflowSystemTask {

    /** 构造器：注册任务类型为 SWITCH */
    public Switch() {
        super(TASK_TYPE_SWITCH);
    }

    /**
     * 执行任务：
     * Switch 本身不需要做任何实际工作，因为它只是一个"分支决策点"。
     * 真正的分支选择由引擎根据任务定义中的 decisionCases / defaultCase 计算得出，
     * 并据此决定后续走哪些任务。
     *
     * 这里直接把任务置为 COMPLETED，返回 true 表示状态已变更、工作流可继续推进。
     */
    @Override
    public boolean execute(
            WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        task.setStatus(TaskModel.Status.COMPLETED);
        return true;
    }
}