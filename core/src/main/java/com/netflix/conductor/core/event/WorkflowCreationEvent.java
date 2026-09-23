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
package com.netflix.conductor.core.event;

import java.io.Serializable;

import com.netflix.conductor.core.execution.StartWorkflowInput;

/**
 * 工作流创建事件
 */
public class WorkflowCreationEvent implements Serializable {

    /**
     * 启动工作流的输入参数
     */
    private final StartWorkflowInput startWorkflowInput;

    /**
     * 构造方法：创建 WorkflowCreationEvent 实例
     */
    public WorkflowCreationEvent(StartWorkflowInput startWorkflowInput) {
        this.startWorkflowInput = startWorkflowInput;
    }

    /**
     * 获取启动工作流的输入参数
     */
    public StartWorkflowInput getStartWorkflowInput() {
        return startWorkflowInput;
    }
}