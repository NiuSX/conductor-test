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
package com.netflix.conductor.core.execution.mapper;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/**
 * HTTPTaskMapper：{@link TaskMapper} 的实现，把 {@link TaskType#HTTP} 类型的
 * {@link WorkflowTask} 映射为状态为 {@link TaskModel.Status#SCHEDULED} 的
 * {@link TaskModel}。
 *
 * 它的职责很简单：构造一个 SCHEDULED 状态的 HTTP 任务实例，交给引擎调度。
 * 真正的 HTTP 调用发生在运行时（由 HttpTask 系统任务执行）。
 */
@Component
public class HTTPTaskMapper implements TaskMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(HTTPTaskMapper.class);

    private final ParametersUtils parametersUtils;
    private final MetadataDAO metadataDAO;

    @Autowired
    public HTTPTaskMapper(ParametersUtils parametersUtils, MetadataDAO metadataDAO) {
        this.parametersUtils = parametersUtils;
        this.metadataDAO = metadataDAO;
    }

    /** 返回本 mapper 处理的任务类型：HTTP */
    @Override
    public String getTaskType() {
        return TaskType.HTTP.name();
    }

    /**
     * 把 HTTP 类型的 {@link WorkflowTask} 映射为 SCHEDULED 状态的 {@link TaskModel}。
     *
     * 流程：
     * 1. 读取 WorkflowTask 和上下文信息
     * 2. 查找 TaskDef（优先用上下文里的，否则查库）
     * 3. 用 ParametersUtils 解析输入参数（支持表达式）
     * 4. 创建 TaskModel，设置输入、状态、重试、延迟、限流等属性
     *
     * @param taskMapperContext 映射上下文，含 WorkflowTask、WorkflowDef、
     *                          WorkflowModel 和 taskId
     * @return 只含一个 HTTP 任务的列表
     * @throws TerminateWorkflowException 任务定义不存在时抛出
     */
    @Override
    public List<TaskModel> getMappedTasks(TaskMapperContext taskMapperContext)
            throws TerminateWorkflowException {

        LOGGER.debug("TaskMapperContext {} in HTTPTaskMapper", taskMapperContext);

        WorkflowTask workflowTask = taskMapperContext.getWorkflowTask();
        // 把 asyncComplete 标记放入输入参数，供运行时判断是否异步完成
        workflowTask.getInputParameters().put("asyncComplete", workflowTask.isAsyncComplete());
        WorkflowModel workflowModel = taskMapperContext.getWorkflowModel();
        String taskId = taskMapperContext.getTaskId();
        int retryCount = taskMapperContext.getRetryCount();

        // 优先用上下文里的 TaskDef，否则查库
        TaskDef taskDefinition =
                Optional.ofNullable(taskMapperContext.getTaskDefinition())
                        .orElseGet(() -> metadataDAO.getTaskDef(workflowTask.getName()));

        // 解析输入参数（支持 ${...} 表达式，从工作流上下文求值）
        Map<String, Object> input =
                parametersUtils.getTaskInputV2(
                        workflowTask.getInputParameters(), workflowModel, taskId, taskDefinition);
        Boolean asynComplete = (Boolean) input.get("asyncComplete");

        // 创建任务实例
        TaskModel httpTask = taskMapperContext.createTaskModel();
        httpTask.setInputData(input);
        httpTask.getInputData().put("asyncComplete", asynComplete);
        httpTask.setStatus(TaskModel.Status.SCHEDULED);
        httpTask.setRetryCount(retryCount);
        // 启动延迟（来自 WorkflowTask 的 startDelay）
        httpTask.setCallbackAfterSeconds(workflowTask.getStartDelay());
        // 若有限流/隔离配置，一并设置
        if (Objects.nonNull(taskDefinition)) {
            httpTask.setRateLimitPerFrequency(taskDefinition.getRateLimitPerFrequency());
            httpTask.setRateLimitFrequencyInSeconds(
                    taskDefinition.getRateLimitFrequencyInSeconds());
            httpTask.setIsolationGroupId(taskDefinition.getIsolationGroupId());
            httpTask.setExecutionNameSpace(taskDefinition.getExecutionNameSpace());
        }
        return List.of(httpTask);
    }
}