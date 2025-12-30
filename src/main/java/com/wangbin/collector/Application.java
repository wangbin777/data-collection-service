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


    /*  1. 基础指标与健康检查：完成系统健康检查 API，涵盖缓存、连接、依赖服务的可用性，顺便建立统一的指标模型（MonitorMetric/HealthStatus），后续模块都复用。
    2. 缓存系统监控：利用 MultiLevelCacheManager 现有统计，先把命中率、容量、过期情况暴露出来，并打通多级缓存的性能指标。
    3. 设备状态与连接监控：基于 ConnectionManager / CollectionManager 的状态事件，实时追踪在线/离线、响应时延、成功率，为后续告警铺路。
    4. 性能/资源监控：实现采集速率、批量任务耗时、调度器效率，以及 JVM/CPU/线程池/磁盘等系统资源指标，可直接使用 JMX + 已有线程池。建议构建统一的性能采样层，方便加指标。
    5. 异常与错误监控：集中采集连接/解析/超时等异常，统计频率与分布，并关联设备、点位，输出聚合报表（也可复用日志聚合）。
    6. 统计报告与趋势分析：在前面指标齐全的基础上，实现实时面板、历史趋势、排行榜与自动报表，需引入持久化策略（时序库或本地归档）。
    按这个顺序开发，每完成一步再继续下一个，既能快速看到结果，又能保证后续模块有数据支撑。需要我从第 1 步开始动手时告诉我。*/


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


