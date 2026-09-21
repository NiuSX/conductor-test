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
package com.netflix.conductor.core.config;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * 调度相关配置类：定义 Conductor 中各种线程池和调度器。
 *
 * 主要提供：
 * - RxJava 的 Scheduler（事件队列轮询用）
 * - Sweeper 线程池（工作流巡检/修复用）
 * - Spring 定时任务线程池（@Scheduled 任务用）
 *
 * @EnableScheduling 开启 Spring 定时任务支持
 * @EnableAsync 开启 Spring 异步方法支持
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableAsync
public class SchedulerConfiguration implements SchedulingConfigurer {

    /** Sweeper 线程池的 Bean 名称 */
    public static final String SWEEPER_EXECUTOR_NAME = "WorkflowSweeperExecutor";

    /**
     * RxJava 调度器，供某些 ObservableQueue 实现使用（如事件队列轮询）。
     *
     * @see com.netflix.conductor.core.events.queue.ConductorObservableQueue
     *
     * @param properties Conductor 配置
     * @return 基于固定线程池的 RxJava Scheduler
     */
    @Bean
    public Scheduler scheduler(ConductorProperties properties) {
        // 线程工厂：给线程命名，便于排查问题
        ThreadFactory threadFactory =
                new BasicThreadFactory.Builder()
                        .namingPattern("event-queue-poll-scheduler-thread-%d")
                        .build();
        // 固定大小线程池，线程数由配置决定
        Executor executorService =
                Executors.newFixedThreadPool(
                        properties.getEventQueueSchedulerPollThreadCount(), threadFactory);

        return Schedulers.from(executorService);
    }

    /**
     * Sweeper 线程池：用于工作流巡检（sweep）和修复（repair）。
     * Sweeper 会周期性扫描"进行中但可能卡住"的工作流，推进或修复它们。
     *
     * @param properties Conductor 配置
     * @return 固定大小线程池
     * @throws IllegalStateException 若配置的线程数 <= 0
     */
    @Bean(SWEEPER_EXECUTOR_NAME)
    public Executor sweeperExecutor(ConductorProperties properties) {
        if (properties.getSweeperThreadCount() <= 0) {
            throw new IllegalStateException(
                    "conductor.app.sweeper-thread-count must be greater than 0.");
        }
        ThreadFactory threadFactory =
                new BasicThreadFactory.Builder().namingPattern("sweeper-thread-%d").build();
        return Executors.newFixedThreadPool(properties.getSweeperThreadCount(), threadFactory);
    }

    /**
     * 配置 Spring @Scheduled 定时任务使用的线程池。
     * 池大小设为 3，与 Conductor 中的定时任务数量对应。
     *
     * @param taskRegistrar Spring 定时任务注册器
     */
    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setPoolSize(3); // 与定时任务数量一致
        threadPoolTaskScheduler.setThreadNamePrefix("scheduled-task-pool-");
        threadPoolTaskScheduler.initialize();
        taskRegistrar.setTaskScheduler(threadPoolTaskScheduler);
    }
}