package com.wangbin.collector;

import com.wangbin.collector.core.collector.manager.CollectionManager;
import com.wangbin.collector.core.config.manager.ConfigManager;
import com.wangbin.collector.core.connection.manager.ConnectionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.bind.annotation.GetMapping;

@SpringBootApplication
@EnableConfigurationProperties
@EnableAsync
@EnableScheduling
@EnableAspectJAutoProxy // 启用AspectJ自动代理并使用CGLIB代理
@Slf4j
public class Application {

    public static void main(String[] args) {
        System.out.println("=== 开始启动 Spring Boot 应用 ===");
        try {
            SpringApplication.run(Application.class, args);
            System.out.println("=== Spring Boot 应用启动成功 ===");
        } catch (Exception e) {
            System.err.println("=== 启动失败: " + e.getMessage() + " ===");
            e.printStackTrace();
        }
    }



    // 优雅停机处理
    /*@Bean
    public GracefulShutdown gracefulShutdown() {
        return new GracefulShutdown();
    }*/

    // 服务初始化
    /*@Bean
    public CommandLineRunner initRunner(
            ConnectionManager connectionManager,
            CollectionManager collectionManager,
            ConfigManager configManager) {
        return args -> {
            // 1. 加载配置
            configManager.init();

            // 2. 初始化连接管理器
            connectionManager.init();

            // 3. 启动采集任务
            collectionManager.startAllTasks();

            // 4. 启动心跳监控
            connectionManager.startHeartbeatMonitor();

            System.out.println("数据采集服务启动完成!");
        };
    }*/
}


