package com.netflix.conductor.core.event;

import java.io.Serializable;

import com.netflix.conductor.model.WorkflowModel;

/**
 * 工作流评估事件。
 *
 * <p>当工作流需要被"重新评估"（即执行一次 decide，决定下一步调度哪些任务或是否终态）时，
 * 由 {@code StartWorkflowOperation} 等组件通过 Spring 的 {@code ApplicationEventPublisher} 发布。
 *
 * <p>它承载了触发本次评估的 {@link WorkflowModel} 快照。监听方（通常是 WorkflowExecutor）
 * 收到事件后，会异步调用 decide 逻辑推进工作流。
 *
 * <p>注意：该事件是瞬时内存事件，不持久化。进程重启后未处理的事件会丢失，
 * 因此生产环境需配合 WorkflowSweeper 兜底扫描防止工作流卡死。
 */
public final class WorkflowEvaluationEvent implements Serializable {

    /** 触发本次评估的工作流实例（含当前状态、任务列表、定义等） */
    private final WorkflowModel workflowModel;

    /**
     * 构造评估事件。
     *
     * @param workflowModel 需要被评估的工作流实例
     */
    public WorkflowEvaluationEvent(WorkflowModel workflowModel) {
        this.workflowModel = workflowModel;
    }

    /**
     * 获取触发本次评估的工作流实例。
     *
     * @return 工作流模型
     */
    public WorkflowModel getWorkflowModel() {
        return workflowModel;
    }
}