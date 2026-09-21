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
package com.netflix.conductor.common.jackson;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

/**
 * JsonProtoModule：可注册到 {@link ObjectMapper} 的 Jackson 模块，
 * 用于实现 ProtoBuf 对象与 JSON 之间的序列化/反序列化。
 *
 * 目前只支持 {@link Any} 这一种 ProtoBuf 类型，因为它是 Conductor 目前
 * 通过 REST API 唯一暴露的 ProtoBuf 对象。
 *
 * 标注为 {@link Component}，让 Spring 自动把它注册到 ObjectMapper。
 *
 * @see AnySerializer
 * @see AnyDeserializer
 * @see org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration
 */
@Component(JsonProtoModule.NAME)
public class JsonProtoModule extends SimpleModule {

    /** 模块名（作为 Spring Bean 名） */
    public static final String NAME = "ConductorJsonProtoModule";

    /** JSON 中表示类型 URL 的字段名 */
    private static final String JSON_TYPE = "@type";
    /** JSON 中表示二进制数据的字段名 */
    private static final String JSON_VALUE = "@value";

    /**
     * AnySerializer：把 ProtoBuf 的 {@link Any} 对象序列化为 JSON。
     *
     * <p>这<b>不是</b> ProtoBuf 的规范 JSON 表示。说明一下我们要解决的问题：
     *
     * <p>{@link Any} 是 ProtoBuf 标准库中的类型，能以类型安全的方式存储任意其他
     * ProtoBuf 消息，即使服务端不知道该消息的 schema。
     *
     * <p>它通过存储一个二元组实现：一个类 URL 的类型声明，以及消息本身的二进制编码。
     * 各语言的 ProtoBuf 实现都提供辅助方法把任意消息编码/解码为 {@link Any}
     * （Java 里是 {@link Any#pack(Message)}）。
     *
     * <p>我们想在 REST API 中暴露这些 {@link Any} 对象，因为它们是 Conductor 新增
     * gRPC 接口的一部分。但问题是，我们<b>无法</b>用 ProtoBuf 的规范 JSON 编码来表示它们。
     * 按官方文档：
     *
     * <p>{@code Any} 的 JSON 表示使用被反序列化后嵌入消息的常规表示，外加一个
     * {@code @type} 字段包含类型 URL。例如：
     *
     * <pre>
     * package google.profile;
     * message Person {
     *   string first_name = 1;
     *   string last_name = 2;
     * }
     * {
     *   "@type": "type.googleapis.com/google.profile.Person",
     *   "firstName": &lt;string&gt;,
     *   "lastName": &lt;string&gt;
     * }
     * </pre>
     *
     * <p>要实现这种表示，PB-JSON 编码器必须知道所有可能被序列化进 {@link Any} 的
     * ProtoBuf 消息类型。而 Conductor 服务端只是透传客户端之间的任意 payload，
     * 无法做到这一点。
     *
     * <p>因此，为了真正通过 REST API 暴露该消息，我们必须自定义一种编码，
     * 包含序列化消息的原始数据（因为服务端无法反序列化它）。我们返回一个
     * 带 {@code '@type'} 和 {@code '@value'} 键的字典：{@code '@type'} 与规范表示
     * 相同，而 {@code '@value'} 包含消息二进制数据的 base64 编码字符串。
     *
     * <p>由于所有官方 Conductor 客户端都要求知道这种编码，所以无论客户端用什么语言，
     * 都能重建出原始的 {@link Any} 消息。
     *
     * @see AnyDeserializer
     */
    @SuppressWarnings("InnerClassMayBeStatic")
    protected class AnySerializer extends JsonSerializer<Any> {

        /**
         * 序列化 Any 为 JSON：
         * {
         *   "@type": "<typeUrl>",
         *   "@value": "<base64 编码的二进制数据>"
         * }
         */
        @Override
        public void serialize(Any value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeStartObject();
            // 写入类型 URL
            jgen.writeStringField(JSON_TYPE, value.getTypeUrl());
            // 写入二进制数据（Jackson 会自动做 base64 编码）
            jgen.writeBinaryField(JSON_VALUE, value.getValue().toByteArray());
            jgen.writeEndObject();
        }
    }

    /**
     * AnyDeserializer：把 {@link Any} 的自定义 JSON 表示还原为原始对象。
     *
     * <p>表示格式详见 {@link AnySerializer}。
     */
    @SuppressWarnings("InnerClassMayBeStatic")
    protected class AnyDeserializer extends JsonDeserializer<Any> {

        /**
         * 从 JSON 反序列化出 Any：
         * 1. 读取 @type 和 @value 字段
         * 2. 校验字段存在且为文本
         * 3. 用 typeUrl + 二进制数据构建 Any
         */
        @Override
        public Any deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode root = p.getCodec().readTree(p);
            JsonNode type = root.get(JSON_TYPE);
            JsonNode value = root.get(JSON_VALUE);

            // 校验 @type 字段
            if (type == null || !type.isTextual()) {
                ctxt.reportMappingException(
                        "invalid '@type' field when deserializing ProtoBuf Any object");
            }

            // 校验 @value 字段
            if (value == null || !value.isTextual()) {
                ctxt.reportMappingException(
                        "invalid '@value' field when deserializing ProtoBuf Any object");
            }

            // 用 typeUrl + 二进制数据重建 Any
            return Any.newBuilder()
                    .setTypeUrl(type.textValue())
                    .setValue(ByteString.copyFrom(value.binaryValue()))
                    .build();
        }
    }

    /**
     * 构造器：注册 Any 的序列化器和反序列化器。
     */
    public JsonProtoModule() {
        super(NAME);
        addSerializer(Any.class, new AnySerializer());
        addDeserializer(Any.class, new AnyDeserializer());
    }
}