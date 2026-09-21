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
package com.netflix.conductor.core;

/**
 * 工作流上下文：用于存储当前线程的认证上下文（客户端应用名和/或用户名）。
 *
 * 它通过 ThreadLocal 实现，使得在同一个请求线程内，任何地方都能通过
 * {@link #get()} 拿到当前调用方的身份，而不需要显式地在方法间传递。
 *
 * 典型用途：
 * - 记录工作流的 ownerApp（谁发起的）
 * - 审计日志
 * - 权限校验
 */
public class WorkflowContext {

    /**
     * 线程本地变量，保存当前线程的 WorkflowContext。
     * 使用 InheritableThreadLocal，使子线程能继承父线程的上下文。
     * 初始值为空上下文（clientApp=""、userName=""）。
     */
    public static final ThreadLocal<WorkflowContext> THREAD_LOCAL =
            InheritableThreadLocal.withInitial(() -> new WorkflowContext("", ""));

    /** 客户端应用名（谁调用的） */
    private final String clientApp;

    /** 用户名（可选） */
    private final String userName;

    /** 构造器：只指定客户端应用名，用户名为 null */
    public WorkflowContext(String clientApp) {
        this.clientApp = clientApp;
        this.userName = null;
    }

    /** 构造器：同时指定客户端应用名和用户名 */
    public WorkflowContext(String clientApp, String userName) {
        this.clientApp = clientApp;
        this.userName = userName;
    }

    /** 获取当前线程的上下文 */
    public static WorkflowContext get() {
        return THREAD_LOCAL.get();
    }

    /** 设置当前线程的上下文 */
    public static void set(WorkflowContext ctx) {
        THREAD_LOCAL.set(ctx);
    }

    /** 清除当前线程的上下文（避免线程池场景下的上下文泄漏） */
    public static void unset() {
        THREAD_LOCAL.remove();
    }

    /**
     * @return 客户端应用名
     */
    public String getClientApp() {
        return clientApp;
    }

    /**
     * @return 用户名
     */
    public String getUserName() {
        return userName;
    }
}