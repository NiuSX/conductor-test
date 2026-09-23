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
package com.netflix.conductor.dao;

import java.util.List;
import java.util.Map;

import com.netflix.conductor.core.events.queue.Message;

/** 负责管理任务队列的 DAO 接口。 */
public interface QueueDAO {

    /**
     * 把一条消息推入队列。
     *
     * @param queueName 队列名称
     * @param id 消息 id
     * @param offsetTimeInSecond 延迟秒数，经过该时间后消息才会变为可见（用于定时队列）
     */
    void push(String queueName, String id, long offsetTimeInSecond);

    /**
     * 把一条带优先级的消息推入队列。
     *
     * @param queueName 队列名称
     * @param id 消息 id
     * @param priority 消息优先级（0 到 99 之间）
     * @param offsetTimeInSecond 延迟秒数，经过该时间后消息才会变为可见（用于定时队列）
     */
    void push(String queueName, String id, int priority, long offsetTimeInSecond);

    /**
     * 批量把消息推入队列。
     *
     * @param queueName 队列名称
     * @param messages 要推入的消息列表
     */
    void push(String queueName, List<Message> messages);

    /**
     * 仅当消息不存在时才推入队列。
     *
     * @param queueName 队列名称
     * @param id 消息 id
     * @param offsetTimeInSecond 延迟秒数，经过该时间后消息才会变为可见（用于定时队列）
     * @return 若消息被成功加入返回 true；若消息已存在返回 false
     */
    boolean pushIfNotExists(String queueName, String id, long offsetTimeInSecond);

    /**
     * 仅当消息不存在时才推入队列（带优先级）。
     *
     * @param queueName 队列名称
     * @param id 消息 id
     * @param priority 消息优先级（0 到 99 之间）
     * @param offsetTimeInSecond 延迟秒数，经过该时间后消息才会变为可见（用于定时队列）
     * @return 若消息被成功加入返回 true；若消息已存在返回 false
     */
    boolean pushIfNotExists(String queueName, String id, int priority, long offsetTimeInSecond);

    /**
     * 从队列中弹出消息 id。
     *
     * @param queueName 队列名称
     * @param count 要读取的消息数量
     * @param timeout 超时时间（毫秒）
     * @return 队列中的消息 id 列表
     */
    List<String> pop(String queueName, int count, int timeout);

    /**
     * 从队列中拉取消息（含消息体）。
     *
     * @param queueName 队列名称
     * @param count 要读取的消息数量
     * @param timeout 超时时间（毫秒）
     * @return 队列中的消息列表
     */
    List<Message> pollMessages(String queueName, int count, int timeout);

    /**
     * 从队列中移除指定消息。
     *
     * @param queueName 队列名称
     * @param messageId 消息 id
     */
    void remove(String queueName, String messageId);

    /**
     * 获取队列当前大小。
     *
     * @param queueName 队列名称
     * @return 队列大小
     */
    int getSize(String queueName);

    /**
     * 确认（ack）消息已处理完成。
     *
     * @param queueName 队列名称
     * @param messageId 消息 id
     * @return 若找到消息并成功 ack 返回 true
     */
    boolean ack(String queueName, String messageId);

    /**
     * 延长未确认消息的租约（延长可见性超时）。
     *
     * @param queueName 队列名称
     * @param messageId 消息 id
     * @param unackTimeout 未确认租约延长的毫秒数（会替换当前值）
     * @return 若成功延长租约返回 true；否则返回 false
     */
    boolean setUnackTimeout(String queueName, String messageId, long unackTimeout);

    /**
     * 清空指定队列。
     *
     * @param queueName 队列名称
     */
    void flush(String queueName);

    /**
     * 获取所有队列的概要信息。
     *
     * @return key 为队列名，value 为队列大小
     */
    Map<String, Long> queuesDetail();

    /**
     * 获取所有队列的详细信息。
     *
     * @return key 为队列名，value 为「分片名 → (大小、未确认队列大小)」的映射
     */
    Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose();

    /** 处理未确认消息（默认空实现，具体实现可按需覆盖）。 */
    default void processUnacks(String queueName) {}

    /**
     * 把消息的延迟时间重置为 0，且不把消息从队列中取出。
     *
     * @param queueName 队列名称
     * @param id 消息 id
     * @return 若消息在队列中且修改成功返回 true，否则返回 false
     */
    boolean resetOffsetTime(String queueName, String id);

    /**
     * 将消息推迟 postponeDurationInSeconds 秒，使其在指定时间内不会被再次拉取。
     * 默认实现为向后兼容采用「先移除再按延迟重新推入」的方式。
     *
     * @param queueName 队列名称
     * @param messageId 消息 id
     * @param priority 消息优先级（0 到 99 之间）
     * @param postponeDurationInSeconds 推迟的秒数
     */
    default boolean postpone(
            String queueName, String messageId, int priority, long postponeDurationInSeconds) {
        remove(queueName, messageId);
        push(queueName, messageId, priority, postponeDurationInSeconds);
        return true;
    }

    /**
     * 检查指定 messageId 的消息是否存在于队列中。
     *
     * @param queueName 队列名称
     * @param messageId 消息 id
     * @return 若存在返回 true
     */
    default boolean containsMessage(String queueName, String messageId) {
        throw new UnsupportedOperationException(
                "Please ensure your provided Queue implementation overrides and implements this method.");
    }
}