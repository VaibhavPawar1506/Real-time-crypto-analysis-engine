package com.crypto.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.support.TaskExecutorAdapter;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.concurrent.Executors;

@SpringBootApplication
@EnableAsync
public class RealTimeCryptoAnalyticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(RealTimeCryptoAnalyticsApplication.class, args);
    }

    /**
     * Explicitly configuring an AsyncTaskExecutor to use Virtual Threads for @Async tasks.
     * Note: spring.threads.virtual.enabled=true already auto-configures the default
     * task execution to use Virtual Threads in Spring Boot 3.2+. This explicit bean
     * is provided as an example of manual configuration if needed.
     */
    @Bean(name = "applicationTaskExecutor")
    @ConditionalOnProperty(prefix = "spring.threads.virtual", name = "enabled", havingValue = "true")
    public AsyncTaskExecutor applicationTaskExecutor() {
        return new TaskExecutorAdapter(Executors.newVirtualThreadPerTaskExecutor());
    }
}
