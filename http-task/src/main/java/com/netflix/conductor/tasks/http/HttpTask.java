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
package com.netflix.conductor.tasks.http;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.utils.Utils;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import com.netflix.conductor.tasks.http.providers.RestTemplateProvider;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_HTTP;

/**
 * HTTP 任务：作为工作流执行的一部分，用于调用另一个 HTTP 端点。
 * 它是 Conductor 的内置系统任务（System Task），不需要外部 Worker 执行。
 */
@Component(TASK_TYPE_HTTP)
public class HttpTask extends WorkflowSystemTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTask.class);

    /** 任务输入中存放 HTTP 请求参数的 key */
    public static final String REQUEST_PARAMETER_NAME = "http_request";

    /** 缺少 HTTP 请求时的错误信息 */
    static final String MISSING_REQUEST =
            "Missing HTTP request. Task input MUST have a '"
                    + REQUEST_PARAMETER_NAME
                    + "' key with HttpTask.Input as value. See documentation for HttpTask for required input parameters";

    /** JSON 反序列化用的类型引用：Map<String, Object> */
    private final TypeReference<Map<String, Object>> mapOfObj =
            new TypeReference<Map<String, Object>>() {};
    /** JSON 反序列化用的类型引用：List<Object> */
    private final TypeReference<List<Object>> listOfObj = new TypeReference<List<Object>>() {};
    protected ObjectMapper objectMapper;
    protected RestTemplateProvider restTemplateProvider;
    /** 请求参数在 inputData 中的 key，默认 "http_request" */
    private final String requestParameter;

    /** Spring 注入构造器：使用默认任务类型 TASK_TYPE_HTTP */
    @Autowired
    public HttpTask(RestTemplateProvider restTemplateProvider, ObjectMapper objectMapper) {
        this(TASK_TYPE_HTTP, restTemplateProvider, objectMapper);
    }

    /** 可指定任务名的构造器，便于子类继承扩展 */
    public HttpTask(
            String name, RestTemplateProvider restTemplateProvider, ObjectMapper objectMapper) {
        super(name);
        this.restTemplateProvider = restTemplateProvider;
        this.objectMapper = objectMapper;
        this.requestParameter = REQUEST_PARAMETER_NAME;
        LOGGER.info("{} initialized...", getTaskType());
    }

    /**
     * 任务启动入口：由 Conductor 引擎调用。
     * 流程：取出 http_request 参数 → 校验 → 发起 HTTP 调用 → 根据响应码设置任务状态。
     */
    @Override
    public void start(WorkflowModel workflow, TaskModel task, WorkflowExecutor executor) {
        // 从任务输入中取出 HTTP 请求定义
        Object request = task.getInputData().get(requestParameter);
        // 记录执行该任务的 workerId（这里是 Conductor 服务器自身）
        task.setWorkerId(Utils.getServerId());
        // 校验：请求参数不能为空
        if (request == null) {
            task.setReasonForIncompletion(MISSING_REQUEST);
            task.setStatus(TaskModel.Status.FAILED);
            return;
        }

        // 将请求参数转换为 Input 对象
        Input input = objectMapper.convertValue(request, Input.class);
        // 校验：URI 不能为空
        if (input.getUri() == null) {
            String reason =
                    "Missing HTTP URI.  See documentation for HttpTask for required input parameters";
            task.setReasonForIncompletion(reason);
            task.setStatus(TaskModel.Status.FAILED);
            return;
        }

        // 校验：HTTP 方法不能为空
        if (input.getMethod() == null) {
            String reason = "No HTTP method specified";
            task.setReasonForIncompletion(reason);
            task.setStatus(TaskModel.Status.FAILED);
            return;
        }

        try {
            // 发起实际的 HTTP 调用
            HttpResponse response = httpCall(input);
            LOGGER.debug(
                    "Response: {}, {}, task:{}",
                    response.statusCode,
                    response.body,
                    task.getTaskId());
            // 2xx 视为成功
            if (response.statusCode > 199 && response.statusCode < 300) {
                // 如果是异步完成任务（如异步回调），则标记为 IN_PROGRESS
                if (isAsyncComplete(task)) {
                    task.setStatus(TaskModel.Status.IN_PROGRESS);
                } else {
                    task.setStatus(TaskModel.Status.COMPLETED);
                }
            } else {
                // 非 2xx 视为失败，记录失败原因
                if (response.body != null) {
                    task.setReasonForIncompletion(response.body.toString());
                } else {
                    task.setReasonForIncompletion("No response from the remote service");
                }
                task.setStatus(TaskModel.Status.FAILED);
            }
            //noinspection ConstantConditions
            // 把响应写入任务输出，供后续任务引用
            if (response != null) {
                task.addOutput("response", response.asMap());
            }

        } catch (Exception e) {
            // 异常兜底：记录日志并标记失败
            LOGGER.error(
                    "Failed to invoke {} task: {} - uri: {}, vipAddress: {} in workflow: {}",
                    getTaskType(),
                    task.getTaskId(),
                    input.getUri(),
                    input.getVipAddress(),
                    task.getWorkflowInstanceId(),
                    e);
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion(
                    "Failed to invoke " + getTaskType() + " task due to: " + e);
            task.addOutput("response", e.toString());
        }
    }

    /**
     * 执行实际的 HTTP 调用。
     *
     * @param input HTTP 请求定义
     * @return HTTP 响应
     * @throws Exception 调用出错时抛出
     * 注意：protected 是为了让继承 HttpTask 的子类能复用此方法发起 HTTP 调用。
     */
    protected HttpResponse httpCall(Input input) throws Exception {
        // 根据 input 获取合适的 RestTemplate（可能带 vipAddress、超时等定制）
        RestTemplate restTemplate = restTemplateProvider.getRestTemplate(input);

        // 组装请求头
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf(input.getContentType()));
        headers.setAccept(Collections.singletonList(MediaType.valueOf(input.getAccept())));

        // 合并用户自定义 header
        input.headers.forEach(
                (key, value) -> {
                    if (value != null) {
                        headers.add(key, value.toString());
                    }
                });

        // 组装请求实体（body + headers）
        HttpEntity<Object> request = new HttpEntity<>(input.getBody(), headers);

        HttpResponse response = new HttpResponse();
        try {
            // 发起 HTTP 请求，响应体以 String 接收后再解析
            ResponseEntity<String> responseEntity =
                    restTemplate.exchange(input.getUri(), input.getMethod(), request, String.class);
            // 仅当 2xx 且有响应体时才解析 body
            if (responseEntity.getStatusCode().is2xxSuccessful() && responseEntity.hasBody()) {
                response.body = extractBody(responseEntity.getBody());
            }

            response.statusCode = responseEntity.getStatusCodeValue();
            response.reasonPhrase = responseEntity.getStatusCode().getReasonPhrase();
            response.headers = responseEntity.getHeaders();
            return response;
        } catch (RestClientException ex) {
            // 网络/客户端异常，记录后抛出
            LOGGER.error(
                    String.format(
                            "Got unexpected http response - uri: %s, vipAddress: %s",
                            input.getUri(), input.getVipAddress()),
                    ex);
            String reason = ex.getLocalizedMessage();
            LOGGER.error(reason, ex);
            throw new Exception(reason);
        }
    }

    /**
     * 解析响应体：根据 JSON 结构返回 List / Map / Number / String。
     * 若不是合法 JSON，则原样返回字符串。
     */
    private Object extractBody(String responseBody) {
        try {
            JsonNode node = objectMapper.readTree(responseBody);
            if (node.isArray()) {
                return objectMapper.convertValue(node, listOfObj);
            } else if (node.isObject()) {
                return objectMapper.convertValue(node, mapOfObj);
            } else if (node.isNumber()) {
                return objectMapper.convertValue(node, Double.class);
            } else {
                return node.asText();
            }
        } catch (IOException jpe) {
            LOGGER.error("Error extracting response body", jpe);
            return responseBody;
        }
    }

    /**
     * 对于系统任务，execute 方法通常返回 false，表示没有额外的异步执行逻辑。
     * 真正的逻辑已在 start() 中完成。
     */
    @Override
    public boolean execute(WorkflowModel workflow, TaskModel task, WorkflowExecutor executor) {
        return false;
    }

    /** 任务取消时的处理：直接把状态置为 CANCELED */
    @Override
    public void cancel(WorkflowModel workflow, TaskModel task, WorkflowExecutor executor) {
        task.setStatus(TaskModel.Status.CANCELED);
    }

    /** 标识该任务为异步任务（由 Conductor 引擎异步调度） */
    @Override
    public boolean isAsync() {
        return true;
    }

    /** HTTP 响应的封装类 */
    public static class HttpResponse {

        public Object body;
        public MultiValueMap<String, String> headers;
        public int statusCode;
        public String reasonPhrase;

        @Override
        public String toString() {
            return "HttpResponse [body="
                    + body
                    + ", headers="
                    + headers
                    + ", statusCode="
                    + statusCode
                    + ", reasonPhrase="
                    + reasonPhrase
                    + "]";
        }

        /** 把响应转换为 Map，便于写入任务输出 */
        public Map<String, Object> asMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("body", body);
            map.put("headers", headers);
            map.put("statusCode", statusCode);
            map.put("reasonPhrase", reasonPhrase);
            return map;
        }
    }

    /** HTTP 请求的输入封装类，对应任务输入中的 http_request 字段 */
    public static class Input {

        private HttpMethod method; // PUT, POST, GET, DELETE, OPTIONS, HEAD
        private String vipAddress;
        private String appName;
        private Map<String, Object> headers = new HashMap<>();
        private String uri;
        private Object body;
        private String accept = MediaType.APPLICATION_JSON_VALUE;
        private String contentType = MediaType.APPLICATION_JSON_VALUE;
        private Integer connectionTimeOut;
        private Integer readTimeOut;

        /** @return HTTP 方法 */
        public HttpMethod getMethod() {
            return method;
        }

        /** @param method 设置 HTTP 方法（字符串形式） */
        public void setMethod(String method) {
            this.method = HttpMethod.valueOf(method);
        }

        /** @return 请求头 */
        public Map<String, Object> getHeaders() {
            return headers;
        }

        /** @param headers 设置请求头 */
        public void setHeaders(Map<String, Object> headers) {
            this.headers = headers;
        }

        /** @return 请求体 */
        public Object getBody() {
            return body;
        }

        /** @param body 设置请求体 */
        public void setBody(Object body) {
            this.body = body;
        }

        /** @return 请求 URI */
        public String getUri() {
            return uri;
        }

        /** @param uri 设置请求 URI */
        public void setUri(String uri) {
            this.uri = uri;
        }

        /** @return VIP 地址（Netflix 内部服务发现用） */
        public String getVipAddress() {
            return vipAddress;
        }

        /** @param vipAddress 设置 VIP 地址 */
        public void setVipAddress(String vipAddress) {
            this.vipAddress = vipAddress;
        }

        /** @return Accept 头 */
        public String getAccept() {
            return accept;
        }

        /** @param accept 设置 Accept 头 */
        public void setAccept(String accept) {
            this.accept = accept;
        }

        /** @return 请求的 MIME 内容类型 */
        public String getContentType() {
            return contentType;
        }

        /** @param contentType 设置请求的 MIME 内容类型 */
        public void setContentType(String contentType) {
            this.contentType = contentType;
        }

        /** @return 应用名 */
        public String getAppName() {
            return appName;
        }

        /** @param appName 设置应用名 */
        public void setAppName(String appName) {
            this.appName = appName;
        }

        /** @return 连接超时（毫秒） */
        public Integer getConnectionTimeOut() {
            return connectionTimeOut;
        }

        /** @return 读取超时（毫秒） */
        public Integer getReadTimeOut() {
            return readTimeOut;
        }

        /** @param connectionTimeOut 设置连接超时 */
        public void setConnectionTimeOut(Integer connectionTimeOut) {
            this.connectionTimeOut = connectionTimeOut;
        }

        /** @param readTimeOut 设置读取超时 */
        public void setReadTimeOut(Integer readTimeOut) {
            this.readTimeOut = readTimeOut;
        }
    }
}