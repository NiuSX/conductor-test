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
package com.netflix.conductor.core.listener;

import com.netflix.conductor.model.WorkflowModel;

/**
 * 工作流状态监听器：用于监听工作流的"完成/终止/最终化"事件。
 *
 * 与 TaskStatusListener 的区别：
 * - TaskStatusListener：监听单个任务的状态变更
 * - WorkflowStatusListener（本接口）：监听整个工作流的状态变更
 *
 * 设计特点：带 "IfEnabled" 的方法会先检查工作流定义里的开关
 * （isWorkflowStatusListenerEnabled），只有开启时才调用对应的回调。
 * 这样使用者可以在定义级别控制是否启用监听，避免不必要的事件开销。
 *
 * 典型用途：
 * - 工作流完成时触发下游流程
 * - 工作流终止时发告警/通知
 * - 审计与合规记录
 */
public interface WorkflowStatusListener {

    /**
     * 工作流完成时的入口（带开关检查）。
     * 仅当工作流定义启用了状态监听时，才调用 onWorkflowCompleted。
     */
    default void onWorkflowCompletedIfEnabled(WorkflowModel workflow) {
        if (workflow.getWorkflowDefinition().isWorkflowStatusListenerEnabled()) {
            onWorkflowCompleted(workflow);
        }
    }

    /**
     * 工作流终止时的入口（带开关检查）。
     * 仅当工作流定义启用了状态监听时，才调用 onWorkflowTerminated。
     */
    default void onWorkflowTerminatedIfEnabled(WorkflowModel workflow) {
        if (workflow.getWorkflowDefinition().isWorkflowStatusListenerEnabled()) {
            onWorkflowTerminated(workflow);
        }
    }

    /**
     * 工作流最终化时的入口（带开关检查）。
     * 仅当工作流定义启用了状态监听时，才调用 onWorkflowFinalized。
     */
    default void onWorkflowFinalizedIfEnabled(WorkflowModel workflow) {
        if (workflow.getWorkflowDefinition().isWorkflowStatusListenerEnabled()) {
            onWorkflowFinalized(workflow);
        }
    }

    /** 工作流完成时调用（无开关检查，由 IfEnabled 方法保证） */
    void onWorkflowCompleted(WorkflowModel workflow);

    /** 工作流终止时调用（无开关检查，由 IfEnabled 方法保证） */
    void onWorkflowTerminated(WorkflowModel workflow);

    /** 工作流最终化时调用（默认空实现，可选覆盖） */
    default void onWorkflowFinalized(WorkflowModel workflow) {}
}