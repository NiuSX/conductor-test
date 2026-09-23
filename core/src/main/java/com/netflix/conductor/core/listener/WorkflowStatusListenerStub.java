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
package com.netflix.conductor.core.listener; // 包声明：Conductor 核心监听器包

import org.slf4j.Logger; // 导入 SLF4J 日志接口
import org.slf4j.LoggerFactory; // 导入 SLF4J 日志工厂

import com.netflix.conductor.model.WorkflowModel; // 导入工作流模型（内部模型）

/**
 * Stub listener default implementation
 *
 * 中文说明：工作流状态监听器的"桩"（Stub）默认实现。
 *
 * <p>所谓 Stub，即一个"占位式"的空实现：所有回调方法只打印 debug 日志，
 * 不做任何实际业务处理。它的作用是：
 * <ul>
 *   <li>作为 {@link WorkflowStatusListener} 接口的默认实现，保证系统在
 *       未配置自定义监听器时也能正常运行（避免空指针）</li>
 *   <li>为开发者提供一个可继承/可参考的模板，按需覆写感兴趣的回调</li>
 *   <li>被 Spring 在缺少其他实现时作为默认 Bean 注入</li>
 * </ul>
 */
public class WorkflowStatusListenerStub implements WorkflowStatusListener {

    // 日志记录器
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowStatusListenerStub.class);

    /**
     * 工作流完成时的回调。
     *
     * <p>中文说明：当工作流正常执行完毕（状态为 COMPLETED）时被触发。
     * 此处仅打印 debug 日志，不做任何业务处理。
     *
     * @param workflow 已完成的工作流模型
     */
    @Override
    public void onWorkflowCompleted(WorkflowModel workflow) {
        LOGGER.debug("Workflow {} is completed", workflow.getWorkflowId());
    }

    /**
     * 工作流终止时的回调。
     *
     * <p>中文说明：当工作流被主动终止（状态为 TERMINATED，如人工取消、
     * 超时终止等）时被触发。此处仅打印 debug 日志，不做任何业务处理。
     *
     * @param workflow 已被终止的工作流模型
     */
    @Override
    public void onWorkflowTerminated(WorkflowModel workflow) {
        LOGGER.debug("Workflow {} is terminated", workflow.getWorkflowId());
    }

    /**
     * 工作流最终确定时的回调。
     *
     * <p>中文说明：当工作流进入最终状态（finalized）时被触发。
     * "Finalized"通常表示工作流已经彻底结束、不再有任何后续处理
     * （例如所有补偿/清理逻辑均已完成）。此处仅打印 debug 日志。
     *
     * @param workflow 已最终确定的工作流模型
     */
    @Override
    public void onWorkflowFinalized(WorkflowModel workflow) {
        LOGGER.debug("Workflow {} is finalized", workflow.getWorkflowId());
    }
}