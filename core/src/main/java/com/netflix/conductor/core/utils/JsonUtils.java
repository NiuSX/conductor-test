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
package com.netflix.conductor.core.utils;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON 工具类：用于解析/展开 JSON 数据。
 *
 * 主要场景：任务输入/输出中，某些字段值可能是"被序列化成字符串的 JSON"
 * （例如嵌套的 JSON 字符串）。这个工具会递归地把这些字符串还原成
 * Java 的 Map / List 对象，方便引擎按结构化数据访问。
 */
@SuppressWarnings("unchecked")
@Component
public class JsonUtils {

    private final ObjectMapper objectMapper;

    public JsonUtils(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * 把 JSON 对象展开为 Java 对象。
     * 根据输入类型分别处理：
     * - List → 递归展开列表元素
     * - Map → 递归展开 map 值
     * - String → 尝试解析为 JSON
     * - 其他 → 原样返回
     *
     * @param input 要展开的对象
     * @return 展开后的对象（包含 Map、List 等 Java 类型）
     */
    public Object expand(Object input) {
        if (input instanceof List) {
            expandList((List<Object>) input);
            return input;
        } else if (input instanceof Map) {
            expandMap((Map<String, Object>) input);
            return input;
        } else if (input instanceof String) {
            return getJson((String) input);
        } else {
            return input;
        }
    }

    /**
     * 递归展开列表中的元素：
     * - 若元素是 JSON 字符串 → 解析成对象
     * - 若元素是 Map → 递归展开
     * - 若元素是 List → 递归展开
     *
     * 注意：这里对 String 的解析结果赋值给了局部变量 value，
     * 并没有写回 list，所以严格来说只对 Map 做了原地修改。
     */
    private void expandList(List<Object> input) {
        for (Object value : input) {
            if (value instanceof String) {
                if (isJsonString(value.toString())) {
                    value = getJson(value.toString());
                }
            } else if (value instanceof Map) {
                expandMap((Map<String, Object>) value);
            } else if (value instanceof List) {
                expandList((List<Object>) value);
            }
        }
    }

    /**
     * 递归展开 Map 中的值：
     * - 若值是 JSON 字符串 → 解析成对象并写回 entry
     * - 若值是 Map → 递归展开
     * - 若值是 List → 递归展开
     */
    private void expandMap(Map<String, Object> input) {
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String) {
                if (isJsonString(value.toString())) {
                    // 解析 JSON 字符串并写回原 entry（原地替换）
                    entry.setValue(getJson(value.toString()));
                }
            } else if (value instanceof Map) {
                expandMap((Map<String, Object>) value);
            } else if (value instanceof List) {
                expandList((List<Object>) value);
            }
        }
    }

    /**
     * 把字符串解析为 JSON 对象。
     *
     * @param jsonAsString 字符串形式的 JSON
     * @return 解析后的对象；若字符串不是合法 JSON，则原样返回，不抛异常
     */
    private Object getJson(String jsonAsString) {
        try {
            return objectMapper.readValue(jsonAsString, Object.class);
        } catch (Exception e) {
            // 解析失败：原样返回，不抛异常
            return jsonAsString;
        }
    }

    /**
     * 简单判断字符串是否"看起来像" JSON：去掉首尾空白后以 { 或 [ 开头。
     * 这是一个廉价的预判，避免对普通字符串做无谓的 JSON 解析。
     *
     * @param jsonAsString 待判断的字符串
     * @return true 表示可能是 JSON
     */
    private boolean isJsonString(String jsonAsString) {
        jsonAsString = jsonAsString.trim();
        return jsonAsString.startsWith("{") || jsonAsString.startsWith("[");
    }
}