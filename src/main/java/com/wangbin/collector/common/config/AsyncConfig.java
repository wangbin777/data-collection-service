package com.wangbin.collector.common.config;

import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class AsyncConfig implements AsyncConfigurer {

    private final Executor asyncExecutor;

    public AsyncConfig(@Qualifier("asyncCollectorExecutor") Executor asyncExecutor) {
        this.asyncExecutor = asyncExecutor;
    }

    @Override
    public Executor getAsyncExecutor() {
        return asyncExecutor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new CustomAsyncExceptionHandler();
    }

    /**
     * 自定义异步异常处理器
     */
    static class CustomAsyncExceptionHandler implements AsyncUncaughtExceptionHandler {

        @Override
        public void handleUncaughtException(Throwable throwable, Method method, Object... params) {
            // 异步任务异常处理
            String errorMsg = String.format("异步任务执行异常: method [%s], params %s",
                    method.getName(), Arrays.toString(params));

            System.err.println(errorMsg);
            throwable.printStackTrace();

            // 这里可以添加日志记录或告警逻辑
            // logger.error(errorMsg, throwable);
            // alertService.sendAsyncErrorAlert(method.getName(), throwable);
        }
    }
}
