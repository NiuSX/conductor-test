/*
 * Copyright 2021 Netflix, Inc.
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
package com.netflix.conductor.common.metadata.tasks;

import java.util.HashSet;
import java.util.Set;

import com.netflix.conductor.annotations.protogen.ProtoEnum;

/**
 * 任务类型枚举：定义 Conductor 支持的所有任务类型。
 *
 * 分为两大类：
 * 1. 系统任务（引擎内置，如 FORK_JOIN、JOIN、SWITCH、WAIT 等）
 * 2. 普通任务（由 Worker 执行，如 SIMPLE、HTTP、LAMBDA 等）
 *
 * @ProtoEnum 标注用于生成 protobuf 定义。
 */
@ProtoEnum
public enum TaskType {
    SIMPLE,              // 最普通的任务，由 Worker 执行
    DYNAMIC,             // 动态任务，运行时决定
    FORK_JOIN,           // 并行分支的起点
    FORK_JOIN_DYNAMIC,   // 动态并行分支
    DECISION,            // 条件分支（已废弃，用 SWITCH 替代）
    SWITCH,              // 条件分支（推荐）
    JOIN,                // 并行分支的汇合点
    DO_WHILE,            // 循环
    SUB_WORKFLOW,        // 子工作流
    START_WORKFLOW,      // 启动另一个工作流
    EVENT,               // 事件任务
    WAIT,                // 等待
    HUMAN,               // 人工任务
    USER_DEFINED,        // 用户自定义（未知类型的兜底）
    HTTP,                // HTTP 调用
    LAMBDA,              // Lambda 函数
    INLINE,              // 内联任务
    EXCLUSIVE_JOIN,      // 排他汇合
    TERMINATE,           // 终止工作流
    KAFKA_PUBLISH,       // 发布 Kafka 消息
    JSON_JQ_TRANSFORM,   // JQ 转换
    SET_VARIABLE,        // 设置变量
    NOOP;                // 空操作

    // ==================== 字符串常量 ====================
    // 动机：避免在代码里硬编码字符串，统一用常量引用。

    /** 条件分支（旧） */
    public static final String TASK_TYPE_DECISION = "DECISION";
    /** 条件分支（新，推荐） */
    public static final String TASK_TYPE_SWITCH = "SWITCH";
    /** 动态任务 */
    public static final String TASK_TYPE_DYNAMIC = "DYNAMIC";
    /** 并行汇合点 */
    public static final String TASK_TYPE_JOIN = "JOIN";
    /** 循环 */
    public static final String TASK_TYPE_DO_WHILE = "DO_WHILE";
    /** 动态并行分支 */
    public static final String TASK_TYPE_FORK_JOIN_DYNAMIC = "FORK_JOIN_DYNAMIC";
    /** 事件任务 */
    public static final String TASK_TYPE_EVENT = "EVENT";
    /** 等待 */
    public static final String TASK_TYPE_WAIT = "WAIT";
    /** 人工任务 */
    public static final String TASK_TYPE_HUMAN = "HUMAN";
    /** 子工作流 */
    public static final String TASK_TYPE_SUB_WORKFLOW = "SUB_WORKFLOW";
    /** 启动工作流 */
    public static final String TASK_TYPE_START_WORKFLOW = "START_WORKFLOW";
    /** 并行分支起点 */
    public static final String TASK_TYPE_FORK_JOIN = "FORK_JOIN";
    /** 普通任务 */
    public static final String TASK_TYPE_SIMPLE = "SIMPLE";
    /** HTTP 调用 */
    public static final String TASK_TYPE_HTTP = "HTTP";
    /** Lambda */
    public static final String TASK_TYPE_LAMBDA = "LAMBDA";
    /** 内联任务 */
    public static final String TASK_TYPE_INLINE = "INLINE";
    /** 排他汇合 */
    public static final String TASK_TYPE_EXCLUSIVE_JOIN = "EXCLUSIVE_JOIN";
    /** 终止工作流 */
    public static final String TASK_TYPE_TERMINATE = "TERMINATE";
    /** Kafka 发布 */
    public static final String TASK_TYPE_KAFKA_PUBLISH = "KAFKA_PUBLISH";
    /** JQ 转换 */
    public static final String TASK_TYPE_JSON_JQ_TRANSFORM = "JSON_JQ_TRANSFORM";
    /** 设置变量 */
    public static final String TASK_TYPE_SET_VARIABLE = "SET_VARIABLE";
    /** 分支（简写） */
    public static final String TASK_TYPE_FORK = "FORK";
    /** 空操作 */
    public static final String TASK_TYPE_NOOP = "NOOP";

    /**
     * 内置任务集合：这些是"控制流任务"，由引擎内置处理，
     * 不通过 Worker 执行，也不可重试。
     *
     * 用于 DeciderService.retry() 中判断是否可重试：
     * 内置任务不可重试。
     */
    private static final Set<String> BUILT_IN_TASKS = new HashSet<>();

    static {
        BUILT_IN_TASKS.add(TASK_TYPE_DECISION);       // 条件分支
        BUILT_IN_TASKS.add(TASK_TYPE_SWITCH);         // 条件分支
        BUILT_IN_TASKS.add(TASK_TYPE_FORK);           // 分支
        BUILT_IN_TASKS.add(TASK_TYPE_JOIN);           // 汇合
        BUILT_IN_TASKS.add(TASK_TYPE_EXCLUSIVE_JOIN); // 排他汇合
        BUILT_IN_TASKS.add(TASK_TYPE_DO_WHILE);       // 循环
    }

    /**
     * 把任务类型字符串转换为 {@link TaskType} 枚举。
     * 对于未知字符串，默认返回 {@link TaskType#USER_DEFINED}。
     *
     * <p>注意：如果不需要 USER_DEFINED 兜底，请直接用 {@link Enum#valueOf(Class, String)}。
     *
     * @param taskType 任务类型字符串
     * @return 对应的 TaskType 枚举，未知则返回 USER_DEFINED
     */
    public static TaskType of(String taskType) {
        try {
            return TaskType.valueOf(taskType);
        } catch (IllegalArgumentException iae) {
            // 未知类型：兜底为 USER_DEFINED
            return TaskType.USER_DEFINED;
        }
    }

    /**
     * 判断某任务类型是否为内置任务（控制流任务）。
     *
     * 内置任务由引擎处理，不可重试。
     *
     * @param taskType 任务类型字符串
     * @return true 表示是内置任务
     */
    public static boolean isBuiltIn(String taskType) {
        return BUILT_IN_TASKS.contains(taskType);
    }
}