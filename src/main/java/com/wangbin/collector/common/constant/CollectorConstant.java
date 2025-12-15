package com.wangbin.collector.common.constant;
/**
 * 采集服务常量
 */
public class CollectorConstant {

    // 服务状态
    public static final int STATUS_ENABLED = 1;
    public static final int STATUS_DISABLED = 0;

    // 默认配置
    public static final int DEFAULT_COLLECTION_INTERVAL = 1000; // 默认采集间隔(ms)
    public static final int DEFAULT_TIMEOUT = 3000; // 默认超时时间(ms)
    public static final int DEFAULT_RETRY_TIMES = 3; // 默认重试次数
    public static final int DEFAULT_MAX_CONNECTIONS = 10; // 默认最大连接数

    // 缓存前缀
    public static final String CACHE_PREFIX_DEVICE = "device:";
    public static final String CACHE_PREFIX_DATA = "data:";
    public static final String CACHE_PREFIX_CONFIG = "config:";
    public static final String CACHE_PREFIX_CONNECTION = "connection:";

    // 队列相关
    public static final int DEFAULT_QUEUE_CAPACITY = 10000;
    public static final int DEFAULT_BATCH_SIZE = 100;

    // 数据质量
    public static final int DATA_QUALITY_GOOD = 100;
    public static final int DATA_QUALITY_BAD = 0;
    public static final int DATA_QUALITY_UNCERTAIN = 50;

    // 时间格式
    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT_MS = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String TIMEZONE = "Asia/Shanghai";

    // 加密算法
    public static final String HASH_ALGORITHM = "SHA-256";
    public static final String ENCRYPT_ALGORITHM = "AES";

    // 网络相关
    public static final int DEFAULT_PORT_HTTP = 80;
    public static final int DEFAULT_PORT_HTTPS = 443;
    public static final int DEFAULT_PORT_TCP = 8080;
    public static final int DEFAULT_PORT_WEBSOCKET = 80;

    // 心跳间隔
    public static final long HEARTBEAT_INTERVAL = 30000L; // 30秒
    public static final long HEARTBEAT_TIMEOUT = 90000L; // 90秒

    // 重连策略
    public static final long RECONNECT_DELAY = 5000L; // 5秒
    public static final int MAX_RECONNECT_TIMES = 3;

    // 监控指标
    public static final String METRIC_COLLECTION_COUNT = "collector.collection.count";
    public static final String METRIC_COLLECTION_TIME = "collector.collection.time";
    public static final String METRIC_CONNECTION_COUNT = "collector.connection.count";
    public static final String METRIC_ERROR_COUNT = "collector.error.count";

    // 线程名称
    public static final String THREAD_NAME_PREFIX_COLLECTION = "collection-";
    public static final String THREAD_NAME_PREFIX_REPORT = "report-";
    public static final String THREAD_NAME_PREFIX_PROCESS = "process-";
    public static final String THREAD_NAME_PREFIX_HEARTBEAT = "heartbeat-";
}
