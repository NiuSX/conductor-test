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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.netflix.conductor.annotations.VisibleForTesting;
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
 * {@link TaskMapper} 的一个实现：把 {@link TaskType#DYNAMIC} 类型的 {@link WorkflowTask}
 * 映射成一个 {@link TaskModel}。
 *
 * <p>与普通任务（SIMPLE，任务名写死在定义里）不同，DYNAMIC 任务的**任务名是运行时决定的**：
 * 定义里只写"从哪个输入参数取任务名"，真正的任务名在运行时从 {@link WorkflowTask#getInputParameters()}
 * 解析出来，再据此查 TaskDef、构造 TaskModel。
 *
 * <p>典型用法：
 * <pre>
 * WorkflowTask {
 *   type = "DYNAMIC",
 *   dynamicTaskNameParam = "taskName",     // 从输入参数 taskName 取真实任务名
 *   inputParameters = { "taskName": "${workflow.input.whichTask}" }
 * }
 * </pre>
 * 运行时若 workflow.input.whichTask = "sendEmail"，则动态任务名 = "sendEmail"，
 * 用它去查 TaskDef，构造一个 taskType = "sendEmail" 的任务。
 */
@Component
public class DynamicTaskMapper implements TaskMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicTaskMapper.class);

    /** 参数解析工具：负责把输入参数中的 ${...} 表达式解析成实际值 */
    private final ParametersUtils parametersUtils;
    /** 元数据 DAO：按任务名查询 TaskDef */
    private final MetadataDAO metadataDAO;

    /** 构造器：注入参数解析工具和元数据 DAO */
    @Autowired
    public DynamicTaskMapper(ParametersUtils parametersUtils, MetadataDAO metadataDAO) {
        this.parametersUtils = parametersUtils;
        this.metadataDAO = metadataDAO;
    }

    /** 本 Mapper 负责的任务类型：DYNAMIC */
    @Override
    public String getTaskType() {
        return TaskType.DYNAMIC.name();
    }

    /**
     * 把一个动态任务映射成 {@link TaskModel}。
     *
     * <p>核心步骤：
     * <ol>
     *   <li>从输入参数中解析出真实任务名（taskName）</li>
     *   <li>用任务名查 TaskDef</li>
     *   <li>解析任务的输入参数（${...} 替换）</li>
     *   <li>构造 TaskModel（SCHEDULED 状态）</li>
     * </ol>
     *
     * @param taskMapperContext 包装了 {@link WorkflowTask}、{@link WorkflowDef}、
     *                          {@link WorkflowModel}、taskId 等上下文
     * @return 只包含一个 {@link TaskModel}（状态 SCHEDULED）的列表
     */
    @Override
    public List<TaskModel> getMappedTasks(TaskMapperContext taskMapperContext)
            throws TerminateWorkflowException {
        LOGGER.debug("TaskMapperContext {} in DynamicTaskMapper", taskMapperContext);

        // 从上下文取出工作流任务定义、已解析的输入、工作流模型、重试信息
        WorkflowTask workflowTask = taskMapperContext.getWorkflowTask();
        Map<String, Object> taskInput = taskMapperContext.getTaskInput();
        WorkflowModel workflowModel = taskMapperContext.getWorkflowModel();
        int retryCount = taskMapperContext.getRetryCount();
        String retriedTaskId = taskMapperContext.getRetryTaskId();

        // 1. 从输入参数中解析出真实任务名
        //    dynamicTaskNameParam 是"从哪个输入参数取任务名"的 key
        String taskNameParam = workflowTask.getDynamicTaskNameParam();
        String taskName = getDynamicTaskName(taskInput, taskNameParam);

        // 2. 把解析出的任务名设置到 workflowTask 上（后续用它查 TaskDef）
        workflowTask.setName(taskName);

        // 3. 用任务名查 TaskDef（找不到则抛 TerminateWorkflowException）
        TaskDef taskDefinition = getDynamicTaskDefinition(workflowTask);
        workflowTask.setTaskDefinition(taskDefinition);

        // 4. 用最终的任务定义，重新解析任务的输入参数（${...} 替换）
        Map<String, Object> input =
                parametersUtils.getTaskInput(
                        workflowTask.getInputParameters(),
                        workflowModel,
                        taskDefinition,
                        taskMapperContext.getTaskId());

        // IMPORTANT: 上面的 workflowTask 被修改过了（name、taskDefinition），
        // 所以 createTaskModel() 必须在这里调用，才能把改动反映到创建的 TaskModel 里。
        TaskModel dynamicTask = taskMapperContext.createTaskModel();

        // 5. 填充任务的各种属性
        dynamicTask.setStartDelayInSeconds(workflowTask.getStartDelay());   // 启动延迟
        dynamicTask.setInputData(input);                                    // 解析后的输入
        dynamicTask.setStatus(TaskModel.Status.SCHEDULED);                  // 初始状态：待调度
        dynamicTask.setRetryCount(retryCount);                              // 重试次数
        dynamicTask.setCallbackAfterSeconds(workflowTask.getStartDelay());  // 回调延迟 = 启动延迟
        dynamicTask.setResponseTimeoutSeconds(taskDefinition.getResponseTimeoutSeconds()); // 响应超时
        dynamicTask.setTaskType(taskName);                                  // ★ 任务类型 = 动态解析出的任务名
        dynamicTask.setRetriedTaskId(retriedTaskId);                        // 被重试的前序任务 id
        dynamicTask.setWorkflowPriority(workflowModel.getPriority());       // 继承工作流优先级

        return Collections.singletonList(dynamicTask);
    }

    /**
     * 从输入参数中解析出动态任务名。
     *
     * @param taskInput     输入参数 map，其中包含 dynamicTaskNameParam 对应的值
     * @param taskNameParam 用于查动态任务名的 key
     * @return 动态任务名
     * @throws TerminateWorkflowException 输入参数里没有对应的任务名
     */
    @VisibleForTesting
    String getDynamicTaskName(Map<String, Object> taskInput, String taskNameParam)
            throws TerminateWorkflowException {
        return Optional.ofNullable(taskInput.get(taskNameParam))
                .map(String::valueOf)   // 转成字符串
                .orElseThrow(
                        () -> {
                            // 找不到任务名参数：抛异常终止工作流
                            String reason =
                                    String.format(
                                            "Cannot map a dynamic task based on the parameter and input. "
                                                    + "Parameter= %s, input= %s",
                                            taskNameParam, taskInput);
                            return new TerminateWorkflowException(reason);
                        });
    }

    /**
     * 根据 {@link WorkflowTask} 获取其 TaskDef。
     *
     * <p>优先用 workflowTask 上已带的 taskDefinition；
     * 若没有，则按 workflowTask.getName() 从 MetadataDAO 查；
     * 还查不到则抛 TerminateWorkflowException。
     *
     * @param workflowTask 已设置好 name 的工作流任务
     * @return TaskDef
     * @throws TerminateWorkflowException 找不到任务定义
     */
    @VisibleForTesting
    TaskDef getDynamicTaskDefinition(WorkflowTask workflowTask)
            throws TerminateWorkflowException {
        // TODO 这是代码库里的常见模式，可以抽到 DAO 层
        return Optional.ofNullable(workflowTask.getTaskDefinition())
                .orElseGet(
                        () ->
                                Optional.ofNullable(metadataDAO.getTaskDef(workflowTask.getName()))
                                        .orElseThrow(
                                                () -> {
                                                    String reason =
                                                            String.format(
                                                                    "Invalid task specified.  Cannot find task by name %s in the task definitions",
                                                                    workflowTask.getName());
                                                    return new TerminateWorkflowException(reason);
                                                }));
    }
}