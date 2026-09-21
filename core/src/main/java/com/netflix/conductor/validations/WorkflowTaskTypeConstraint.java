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
package com.netflix.conductor.validations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.text.ParseException;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Optional;

import javax.script.ScriptException;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

import org.apache.commons.lang3.StringUtils;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.events.ScriptEvaluator;
import com.netflix.conductor.core.utils.DateTimeUtils;

import static com.netflix.conductor.core.execution.tasks.Terminate.getTerminationStatusParameter;
import static com.netflix.conductor.core.execution.tasks.Terminate.validateInputStatus;
import static com.netflix.conductor.core.execution.tasks.Wait.DURATION_INPUT;
import static com.netflix.conductor.core.execution.tasks.Wait.UNTIL_INPUT;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.TYPE;

/**
 * 工作流任务类型约束：一个自定义的 Bean Validation 注解。
 *
 * 它的作用是**在 WorkflowTask 定义阶段，校验各任务类型所需的参数是否齐全**。
 * 例如：SWITCH 任务必须有 evaluatorType 和 expression，WAIT 任务必须提供
 * duration 或 until 之一，等等。
 *
 * 设计要点：
 * - 这是一个"类级别"注解（@Target({TYPE, ANNOTATION_TYPE})），作用于 WorkflowTask 对象整体
 * - 校验逻辑委托给内部类 WorkflowTaskValidator
 * - 通过 switch 按任务类型分发到不同的校验方法
 * - 校验失败时把错误信息加入 ConstraintValidatorContext，而不是抛异常
 *
 * 这样可以在工作流定义注册/启动前就发现配置错误，避免运行时才失败。
 */
@Documented
@Constraint(validatedBy = WorkflowTaskTypeConstraint.WorkflowTaskValidator.class)
@Target({TYPE, ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface WorkflowTaskTypeConstraint {

    /** 默认错误消息（实际消息在校验器里动态生成） */
    String message() default "";

    /** 校验分组 */
    Class<?>[] groups() default {};

    /** 负载 */
    Class<? extends Payload>[] payload() default {};

    /**
     * 校验器实现：按任务类型分发到对应的参数校验方法。
     */
    class WorkflowTaskValidator
            implements ConstraintValidator<WorkflowTaskTypeConstraint, WorkflowTask> {

        /** 错误消息模板：%s 字段是 taskType: %s taskName: %s 所必需的 */
        final String PARAM_REQUIRED_STRING_FORMAT =
                "%s field is required for taskType: %s taskName: %s";

        @Override
        public void initialize(WorkflowTaskTypeConstraint constraintAnnotation) {}

        /**
         * 校验入口：按任务类型分发到具体校验方法。
         * 未知类型不做校验（返回 true）。
         */
        @Override
        public boolean isValid(WorkflowTask workflowTask, ConstraintValidatorContext context) {
            // 禁用默认约束违例，改用自定义消息
            context.disableDefaultConstraintViolation();

            boolean valid = true;

            // 按任务类型校验所需参数是否设置
            switch (workflowTask.getType()) {
                case TaskType.TASK_TYPE_EVENT:
                    valid = isEventTaskValid(workflowTask, context);
                    break;
                case TaskType.TASK_TYPE_DECISION:
                    valid = isDecisionTaskValid(workflowTask, context);
                    break;
                case TaskType.TASK_TYPE_SWITCH:
                    valid = isSwitchTaskValid(workflowTask, context);
                    break;
                case TaskType.TASK_TYPE_DYNAMIC:
                    valid = isDynamicTaskValid(workflowTask, context);
                    break;
                case TaskType.TASK_TYPE_FORK_JOIN_DYNAMIC:
                    valid = isDynamicForkJoinValid(workflowTask, context);
                    break;
                case TaskType.TASK_TYPE_HTTP:
                    valid = isHttpTaskValid(workflowTask, context);
                    break;
                case TaskType.TASK_TYPE_FORK_JOIN:
                    valid = isForkJoinTaskValid(workflowTask, context);
                    break;
                case TaskType.TASK_TYPE_TERMINATE:
                    valid = isTerminateTaskValid(workflowTask, context);
                    break;
                case TaskType.TASK_TYPE_KAFKA_PUBLISH:
                    valid = isKafkaPublishTaskValid(workflowTask, context);
                    break;
                case TaskType.TASK_TYPE_DO_WHILE:
                    valid = isDoWhileTaskValid(workflowTask, context);
                    break;
                case TaskType.TASK_TYPE_SUB_WORKFLOW:
                    valid = isSubWorkflowTaskValid(workflowTask, context);
                    break;
                case TaskType.TASK_TYPE_JSON_JQ_TRANSFORM:
                    valid = isJSONJQTransformTaskValid(workflowTask, context);
                    break;
                case TaskType.TASK_TYPE_WAIT:
                    valid = isWaitTaskValid(workflowTask, context);
                    break;
            }

            return valid;
        }

        // ==================== 各任务类型的校验方法 ====================

        /** EVENT 任务：必须设置 sink（事件接收端） */
        private boolean isEventTaskValid(
                WorkflowTask workflowTask, ConstraintValidatorContext context) {
            boolean valid = true;
            if (workflowTask.getSink() == null) {
                String message =
                        String.format(
                                PARAM_REQUIRED_STRING_FORMAT,
                                "sink",
                                TaskType.TASK_TYPE_EVENT,
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }
            return valid;
        }

        /**
         * DECISION 任务（已废弃）：必须设置 caseValueParam 或 caseExpression，
         * decisionCases 不能为空；若用 caseExpression 则表达式必须合法。
         */
        private boolean isDecisionTaskValid(
                WorkflowTask workflowTask, ConstraintValidatorContext context) {
            boolean valid = true;
            // 必须二选一：caseValueParam 或 caseExpression
            if (workflowTask.getCaseValueParam() == null
                    && workflowTask.getCaseExpression() == null) {
                String message =
                        String.format(
                                PARAM_REQUIRED_STRING_FORMAT,
                                "caseValueParam or caseExpression",
                                TaskType.DECISION,
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }
            // decisionCases 必须存在且非空
            if (workflowTask.getDecisionCases() == null) {
                String message =
                        String.format(
                                PARAM_REQUIRED_STRING_FORMAT,
                                "decisionCases",
                                TaskType.DECISION,
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            } else if ((workflowTask.getDecisionCases() != null
                    || workflowTask.getCaseExpression() != null)
                    && (workflowTask.getDecisionCases().size() == 0)) {
                String message =
                        String.format(
                                "decisionCases should have atleast one task for taskType: %s taskName: %s",
                                TaskType.DECISION, workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }

            // 若用 caseExpression，校验表达式语法
            if (workflowTask.getCaseExpression() != null) {
                try {
                    validateScriptExpression(
                            workflowTask.getCaseExpression(), workflowTask.getInputParameters());
                } catch (Exception ee) {
                    String message =
                            String.format(
                                    ee.getMessage() + ", taskType: DECISION taskName %s",
                                    workflowTask.getName());
                    context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                    valid = false;
                }
            }

            return valid;
        }

        /** 校验脚本表达式是否能正常求值（用空输入试跑） */
        private void validateScriptExpression(
                String expression, Map<String, Object> inputParameters) {
            try {
                Object returnValue = ScriptEvaluator.eval(expression, inputParameters);
            } catch (ScriptException e) {
                throw new IllegalArgumentException(
                        String.format("Expression is not well formatted: %s", e.getMessage()));
            }
        }

        /**
         * SWITCH 任务：必须设置 evaluatorType、expression，decisionCases 非空；
         * 若 evaluatorType 是 javascript 则表达式必须合法。
         */
        private boolean isSwitchTaskValid(
                WorkflowTask workflowTask, ConstraintValidatorContext context) {
            boolean valid = true;
            if (workflowTask.getEvaluatorType() == null) {
                String message =
                        String.format(
                                PARAM_REQUIRED_STRING_FORMAT,
                                "evaluatorType",
                                TaskType.SWITCH,
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            } else if (workflowTask.getExpression() == null) {
                String message =
                        String.format(
                                PARAM_REQUIRED_STRING_FORMAT,
                                "expression",
                                TaskType.SWITCH,
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }
            if (workflowTask.getDecisionCases() == null) {
                String message =
                        String.format(
                                PARAM_REQUIRED_STRING_FORMAT,
                                "decisionCases",
                                TaskType.SWITCH,
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            } else if (workflowTask.getDecisionCases() != null
                    && workflowTask.getDecisionCases().size() == 0) {
                String message =
                        String.format(
                                "decisionCases should have atleast one task for taskType: %s taskName: %s",
                                TaskType.SWITCH, workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }

            // javascript 类型的表达式需要语法校验
            if ("javascript".equals(workflowTask.getEvaluatorType())
                    && workflowTask.getExpression() != null) {
                try {
                    validateScriptExpression(
                            workflowTask.getExpression(), workflowTask.getInputParameters());
                } catch (Exception ee) {
                    String message =
                            String.format(
                                    ee.getMessage() + ", taskType: SWITCH taskName %s",
                                    workflowTask.getName());
                    context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                    valid = false;
                }
            }
            return valid;
        }

        /** DO_WHILE 任务：必须有 loopCondition（循环条件）和 loopOver（循环体，非空） */
        private boolean isDoWhileTaskValid(
                WorkflowTask workflowTask, ConstraintValidatorContext context) {
            boolean valid = true;
            if (workflowTask.getLoopCondition() == null) {
                String message =
                        String.format(
                                PARAM_REQUIRED_STRING_FORMAT,
                                "loopExpression",
                                TaskType.DO_WHILE,
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }
            if (workflowTask.getLoopOver() == null || workflowTask.getLoopOver().size() == 0) {
                String message =
                        String.format(
                                PARAM_REQUIRED_STRING_FORMAT,
                                "loopover",
                                TaskType.DO_WHILE,
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }
            return valid;
        }

        /** DYNAMIC 任务：必须有 dynamicTaskNameParam（动态任务名参数） */
        private boolean isDynamicTaskValid(
                WorkflowTask workflowTask, ConstraintValidatorContext context) {
            boolean valid = true;
            if (workflowTask.getDynamicTaskNameParam() == null) {
                String message =
                        String.format(
                                PARAM_REQUIRED_STRING_FORMAT,
                                "dynamicTaskNameParam",
                                TaskType.DYNAMIC,
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }

            return valid;
        }

        /**
         * WAIT 任务：duration 和 until 必须二选一（不能同时设置）；
         * 非表达式形式的值必须能被正确解析为时长/日期。
         */
        private boolean isWaitTaskValid(
                WorkflowTask workflowTask, ConstraintValidatorContext context) {
            boolean valid = true;
            String duration =
                    Optional.ofNullable(workflowTask.getInputParameters().get(DURATION_INPUT))
                            .orElse("")
                            .toString();
            String until =
                    Optional.ofNullable(workflowTask.getInputParameters().get(UNTIL_INPUT))
                            .orElse("")
                            .toString();

            // duration 和 until 不能同时设置
            if (StringUtils.isNotBlank(duration) && StringUtils.isNotBlank(until)) {
                String message =
                        "Both 'duration' and 'until' specified. Please provide only one input";
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }

            try {
                // 非表达式形式（不以 ${ 开头）才做解析校验
                if (StringUtils.isNotBlank(duration) && !(duration.startsWith("${"))) {
                    DateTimeUtils.parseDuration(duration);
                } else if (StringUtils.isNotBlank(until) && !(until.startsWith("${"))) {
                    DateTimeUtils.parseDate(until);
                }
            } catch (DateTimeParseException e) {
                String message = "Unable to parse date ";
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            } catch (IllegalArgumentException e) {
                String message = "Either date or duration is passed as null ";
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            } catch (ParseException e) {
                String message = "Unable to parse date ";
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            } catch (Exception e) {
                String message = "Wait time specified is invalid.  The duration must be in ";
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }

            return valid;
        }

        /**
         * FORK_JOIN_DYNAMIC 任务：
         * - dynamicForkJoinTasksParam 与 (dynamicForkTasksParam + dynamicForkTasksInputParamName) 互斥
         * - 若不用 dynamicForkJoinTasksParam，则必须同时提供 dynamicForkTasksParam 和 dynamicForkTasksInputParamName
         */
        private boolean isDynamicForkJoinValid(
                WorkflowTask workflowTask, ConstraintValidatorContext context) {
            boolean valid = true;

            // 两套参数不能混用
            if (workflowTask.getDynamicForkJoinTasksParam() != null
                    && (workflowTask.getDynamicForkTasksParam() != null
                    || workflowTask.getDynamicForkTasksInputParamName() != null)) {
                String message =
                        String.format(
                                "dynamicForkJoinTasksParam or combination of dynamicForkTasksInputParamName and dynamicForkTasksParam cam be used for taskType: %s taskName: %s",
                                TaskType.FORK_JOIN_DYNAMIC, workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                return false;
            }

            if (workflowTask.getDynamicForkJoinTasksParam() != null) {
                // 用 dynamicForkJoinTasksParam 即可，直接通过
                return valid;
            } else {
                // 否则必须同时提供两个参数
                if (workflowTask.getDynamicForkTasksParam() == null) {
                    String message =
                            String.format(
                                    PARAM_REQUIRED_STRING_FORMAT,
                                    "dynamicForkTasksParam",
                                    TaskType.FORK_JOIN_DYNAMIC,
                                    workflowTask.getName());
                    context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                    valid = false;
                }
                if (workflowTask.getDynamicForkTasksInputParamName() == null) {
                    String message =
                            String.format(
                                    PARAM_REQUIRED_STRING_FORMAT,
                                    "dynamicForkTasksInputParamName",
                                    TaskType.FORK_JOIN_DYNAMIC,
                                    workflowTask.getName());
                    context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                    valid = false;
                }
            }

            return valid;
        }

        /**
         * HTTP 任务：inputParameters 或 TaskDef.inputTemplate 中必须有 http_request。
         */
        private boolean isHttpTaskValid(
                WorkflowTask workflowTask, ConstraintValidatorContext context) {
            boolean valid = true;
            boolean isInputParameterSet = false;
            boolean isInputTemplateSet = false;

            // 检查 inputParameters 里是否有 http_request
            if (workflowTask.getInputParameters() != null
                    && workflowTask.getInputParameters().containsKey("http_request")) {
                isInputParameterSet = true;
            }

            // 检查 TaskDef.inputTemplate 里是否有 http_request
            TaskDef taskDef =
                    Optional.ofNullable(workflowTask.getTaskDefinition())
                            .orElse(
                                    ValidationContext.getMetadataDAO()
                                            .getTaskDef(workflowTask.getName()));

            if (taskDef != null
                    && taskDef.getInputTemplate() != null
                    && taskDef.getInputTemplate().containsKey("http_request")) {
                isInputTemplateSet = true;
            }

            // 两处都没有则报错
            if (!(isInputParameterSet || isInputTemplateSet)) {
                String message =
                        String.format(
                                PARAM_REQUIRED_STRING_FORMAT,
                                "inputParameters.http_request",
                                TaskType.HTTP,
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }

            return valid;
        }

        /** FORK_JOIN 任务：forkTasks 若存在则不能为空 */
        private boolean isForkJoinTaskValid(
                WorkflowTask workflowTask, ConstraintValidatorContext context) {
            boolean valid = true;

            if (workflowTask.getForkTasks() != null && (workflowTask.getForkTasks().size() == 0)) {
                String message =
                        String.format(
                                "forkTasks should have atleast one task for taskType: %s taskName: %s",
                                TaskType.FORK_JOIN, workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }

            return valid;
        }

        /**
         * TERMINATE 任务：不能是 optional；必须提供 terminationStatus 参数且值为 COMPLETED 或 FAILED。
         */
        private boolean isTerminateTaskValid(
                WorkflowTask workflowTask, ConstraintValidatorContext context) {
            boolean valid = true;
            Object inputStatusParam =
                    workflowTask.getInputParameters().get(getTerminationStatusParameter());
            // TERMINATE 任务不能是 optional
            if (workflowTask.isOptional()) {
                String message =
                        String.format(
                                "terminate task cannot be optional, taskName: %s",
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }
            // 必须有合法的 terminationStatus
            if (inputStatusParam == null || !validateInputStatus(inputStatusParam.toString())) {
                String message =
                        String.format(
                                "terminate task must have an %s parameter and must be set to COMPLETED or FAILED, taskName: %s",
                                getTerminationStatusParameter(), workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }
            return valid;
        }

        /**
         * KAFKA_PUBLISH 任务：inputParameters 或 TaskDef.inputTemplate 中必须有 kafka_request。
         */
        private boolean isKafkaPublishTaskValid(
                WorkflowTask workflowTask, ConstraintValidatorContext context) {
            boolean valid = true;
            boolean isInputParameterSet = false;
            boolean isInputTemplateSet = false;

            if (workflowTask.getInputParameters() != null
                    && workflowTask.getInputParameters().containsKey("kafka_request")) {
                isInputParameterSet = true;
            }

            TaskDef taskDef =
                    Optional.ofNullable(workflowTask.getTaskDefinition())
                            .orElse(
                                    ValidationContext.getMetadataDAO()
                                            .getTaskDef(workflowTask.getName()));

            if (taskDef != null
                    && taskDef.getInputTemplate() != null
                    && taskDef.getInputTemplate().containsKey("kafka_request")) {
                isInputTemplateSet = true;
            }

            if (!(isInputParameterSet || isInputTemplateSet)) {
                String message =
                        String.format(
                                PARAM_REQUIRED_STRING_FORMAT,
                                "inputParameters.kafka_request",
                                TaskType.KAFKA_PUBLISH,
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }

            return valid;
        }

        /** SUB_WORKFLOW 任务：必须有 subWorkflowParam */
        private boolean isSubWorkflowTaskValid(
                WorkflowTask workflowTask, ConstraintValidatorContext context) {
            boolean valid = true;
            if (workflowTask.getSubWorkflowParam() == null) {
                String message =
                        String.format(
                                PARAM_REQUIRED_STRING_FORMAT,
                                "subWorkflowParam",
                                TaskType.SUB_WORKFLOW,
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }
            return valid;
        }

        /**
         * JSON_JQ_TRANSFORM 任务：inputParameters 或 TaskDef.inputTemplate 中必须有 queryExpression。
         */
        private boolean isJSONJQTransformTaskValid(
                WorkflowTask workflowTask, ConstraintValidatorContext context) {
            boolean valid = true;
            boolean isInputParameterSet = false;
            boolean isInputTemplateSet = false;

            if (workflowTask.getInputParameters() != null
                    && workflowTask.getInputParameters().containsKey("queryExpression")) {
                isInputParameterSet = true;
            }

            TaskDef taskDef =
                    Optional.ofNullable(workflowTask.getTaskDefinition())
                            .orElse(
                                    ValidationContext.getMetadataDAO()
                                            .getTaskDef(workflowTask.getName()));

            if (taskDef != null
                    && taskDef.getInputTemplate() != null
                    && taskDef.getInputTemplate().containsKey("queryExpression")) {
                isInputTemplateSet = true;
            }

            if (!(isInputParameterSet || isInputTemplateSet)) {
                String message =
                        String.format(
                                PARAM_REQUIRED_STRING_FORMAT,
                                "inputParameters.queryExpression",
                                TaskType.JSON_JQ_TRANSFORM,
                                workflowTask.getName());
                context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
                valid = false;
            }

            return valid;
        }
    }
}