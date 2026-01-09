package com.wangbin.collector.core.collector.scheduler;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.collector.manager.CollectionManager;
import com.wangbin.collector.core.collector.statistics.CollectionStatistics;
import com.wangbin.collector.core.config.manager.ConfigManager;
import com.wangbin.collector.core.config.model.ConfigUpdateEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 高性能采集调度器 - 支持3000点/秒的高频采集
 * 采用时间片调度 + 异步流水线 + 智能批量的优化架构
 */
@Slf4j
@Service
public class CollectionScheduler {

    @Autowired
    private CollectionManager collectionManager;

    @Autowired
    private ConfigManager configManager;

    @Autowired
    private CollectionStatistics collectionStatistics;

    // ================== 优化的线程池配置 ==================

    // 1. 时间片调度器 - 负责宏观调度
    private ScheduledExecutorService timeSliceScheduler;

    // 2. 批量任务分发器 - 负责任务分发
    private ExecutorService batchDispatcher;

    // 3. 异步采集执行器 - 负责实际采集（IO密集型）
    private ThreadPoolExecutor asyncCollectorPool;

    // 4. 数据处理执行器 - 负责数据处理（CPU密集型）
    private ThreadPoolExecutor dataProcessorPool;

    // ================== 调度数据结构 ==================

    // 设备调度状态：deviceId -> 调度信息
    private final Map<String, DeviceScheduleInfo> deviceScheduleInfo = new ConcurrentHashMap<>();

    // 时间片任务队列：timeSliceIndex -> 设备批次列表
    private final Map<Integer, List<DeviceBatchTask>> timeSliceTasks = new ConcurrentHashMap<>();

    // 采集任务队列：deviceId -> 待执行批次
    private final Map<String, LinkedBlockingQueue<BatchTask>> deviceTaskQueues = new ConcurrentHashMap<>();

    // 活跃连接池：deviceId -> 连接状态
    private final Map<String, ConnectionState> connectionStates = new ConcurrentHashMap<>();

    // 性能统计
    private final PerformanceMonitor performanceMonitor = new PerformanceMonitor();

    // 调度锁，防止重复调度
    private final ReentrantLock scheduleLock = new ReentrantLock();

    @Value("${collector.scheduler.initial-time-slice-count:2}")
    private int initialTimeSliceCount;

    @Value("${collector.scheduler.max-time-slice-count:10}")
    private int maxTimeSliceCount;

    @Value("${collector.scheduler.min-time-slice-interval-ms:50}")
    private int minTimeSliceInterval;

    @Value("${collector.scheduler.default-time-slice-interval-ms:1000}")
    private int defaultTimeSliceInterval;

    @Value("${collector.scheduler.initial-time-slice-interval-ms:1000}")
    private int initialTimeSliceInterval;

    @Value("${collector.scheduler.dynamic-adjust-interval-ms:30000}")
    private int dynamicAdjustInterval;

    @Value("${collector.scheduler.collect-timeout-ms:500}")
    private long collectTimeoutMs;
    
    // 自适应采集配置
    @Value("${collector.adaptive-collection.enabled:true}")
    private boolean adaptiveCollectionEnabled;
    
    @Value("${collector.adaptive-collection.adjust-window-ms:60000}")
    private long adaptiveAdjustWindow;
    
    // 时间片配置
    private AtomicInteger TIME_SLICE_COUNT = new AtomicInteger(2);          // 动态时间片数量
    private AtomicInteger TIME_SLICE_INTERVAL = new AtomicInteger(1000);    // 动态时间片间隔（ms）
    private TimeSliceTuner timeSliceTuner;

    @PostConstruct
    public void init() {
        // 获取服务器CPU核心数
        int cpuCores = Runtime.getRuntime().availableProcessors();
        int normalizedSliceCount = Math.max(1, Math.min(initialTimeSliceCount, maxTimeSliceCount));
        TIME_SLICE_COUNT.set(normalizedSliceCount);
        int normalizedInterval = Math.max(minTimeSliceInterval, initialTimeSliceInterval);
        TIME_SLICE_INTERVAL.set(normalizedInterval);
        int maxInterval = Math.max(defaultTimeSliceInterval * 2, normalizedInterval);
        this.timeSliceTuner = new TimeSliceTuner(minTimeSliceInterval, maxInterval, normalizedInterval);

        // ============ 优化的线程池初始化 ============

        // 1. 时间片调度器（核心调度，不宜过多）
        timeSliceScheduler = new ScheduledThreadPoolExecutor(
                Math.max(2, cpuCores / 4),  // 根据CPU核心数调整
                new NamedThreadFactory("time-slice-scheduler")
        );

        // 2. 批量任务分发器
        batchDispatcher = new ThreadPoolExecutor(
                cpuCores,                    // 核心线程数 = CPU核心数
                cpuCores * 2,                // 最大线程数
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                new NamedThreadFactory("batch-dispatcher")
        );

        // 3. 异步采集执行器（IO密集型，可设置较多线程）
        asyncCollectorPool = new ThreadPoolExecutor(
                cpuCores * 4,                // 核心线程数
                cpuCores * 8,                // 最大线程数
                30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10000),
                new NamedThreadFactory("async-collector") {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = super.newThread(r);
                        thread.setDaemon(true);  // 设为守护线程
                        return thread;
                    }
                }
        );

        // 4. 数据处理执行器（CPU密集型）
        dataProcessorPool = new ThreadPoolExecutor(
                cpuCores,                    // 核心线程数 = CPU核心数
                cpuCores * 2,                // 最大线程数
                30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(5000),
                new NamedThreadFactory("data-processor")
        );

        // 初始化时间片任务表
        for (int i = 0; i < TIME_SLICE_COUNT.get(); i++) {
            timeSliceTasks.put(i, new CopyOnWriteArrayList<>());
        }

        // 启动时间片调度
        startTimeSliceScheduling();

        // 启动动态时间片调整
        startDynamicTimeSliceAdjustment();

        // 启动性能监控
        startPerformanceMonitoring();

        log.info("高性能采集调度器初始化完成，CPU核心数: {}, 初始时间片数: {}", cpuCores, TIME_SLICE_COUNT.get());

        // 延迟启动所有设备
        timeSliceScheduler.schedule(this::autoStartAllDevices, 5, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void destroy() {
        log.info("开始销毁高性能采集调度器...");

        // 停止所有设备
        stopAllDevices();

        // 关闭线程池
        shutdownExecutor("时间片调度器", timeSliceScheduler);
        shutdownExecutor("批量分发器", batchDispatcher);
        shutdownExecutor("异步采集器", asyncCollectorPool);
        shutdownExecutor("数据处理器", dataProcessorPool);

        // 清空数据结构
        deviceScheduleInfo.clear();
        timeSliceTasks.clear();
        deviceTaskQueues.clear();
        connectionStates.clear();

        log.info("高性能采集调度器销毁完成");
    }

    /**
     * 启动时间片调度
     */
    private void startTimeSliceScheduling() {
        int sliceCount = TIME_SLICE_COUNT.get();
        int sliceInterval = TIME_SLICE_INTERVAL.get();
        
        for (int sliceIndex = 0; sliceIndex < sliceCount; sliceIndex++) {
            final int currentSlice = sliceIndex;

            // 每个时间片启动一个调度任务
            timeSliceScheduler.scheduleAtFixedRate(() -> {
                try {
                    executeTimeSlice(currentSlice);
                } catch (Exception e) {
                    log.error("时间片 {} 执行异常", currentSlice, e);
                }
            }, sliceIndex * sliceInterval, sliceInterval * sliceCount, TimeUnit.MILLISECONDS);
        }

        log.info("时间片调度已启动，共 {} 个时间片，每片 {}ms", sliceCount, sliceInterval);
    }

    /**
     * 执行指定时间片的任务
     */
    private void executeTimeSlice(int sliceIndex) {
        long startTime = System.currentTimeMillis();
        int currentSliceInterval = TIME_SLICE_INTERVAL.get();

        try {
            List<DeviceBatchTask> tasks = timeSliceTasks.get(sliceIndex);
            if (tasks == null || tasks.isEmpty()) {
                return;
            }

            // 并发执行本时间片的所有任务
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (DeviceBatchTask task : tasks) {
                if (task.shouldSkip()) {
                    continue;
                }

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        processDeviceBatch(task);
                    } catch (Exception e) {
                        log.error("处理设备批次失败: {}", task.deviceId, e);
                    }
                }, batchDispatcher);

                futures.add(future);
            }

            // 等待本时间片所有任务完成（有超时限制）
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(currentSliceInterval - 10, TimeUnit.MILLISECONDS); // 留10ms余量
            } catch (TimeoutException e) {
                log.warn("时间片 {} 执行超时", sliceIndex);
            } catch (Exception e) {
                log.error("时间片 {} 执行异常", sliceIndex, e);
            }

        } finally {
            long executionTime = System.currentTimeMillis() - startTime;
            performanceMonitor.recordTimeSliceExecution(sliceIndex, executionTime);

            if (executionTime > currentSliceInterval) {
                log.warn("时间片 {} 执行时间 {}ms 超过限制 {}ms",
                        sliceIndex, executionTime, currentSliceInterval);
            }
        }
    }

    /**
     * 处理设备批次
     */
    private void processDeviceBatch(DeviceBatchTask batchTask) {
        String deviceId = batchTask.deviceId;
        List<DataPoint> points = batchTask.points;

        long startTime = System.currentTimeMillis();
        boolean success = false;

        try {
            // 检查设备连接状态
            ConnectionState connState = connectionStates.get(deviceId);
            if (connState == null || !connState.isConnected()) {
                if (!reconnectDevice(deviceId)) {
                    log.warn("设备 {} 连接失败，跳过本批次", deviceId);
                    return;
                }
            }

            // 异步采集执行
            CompletableFuture<Map<String, Object>> collectFuture =
                    CompletableFuture.supplyAsync(() -> {
                        try {
                            return collectionManager.readPoints(deviceId, points);
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    }, asyncCollectorPool);

            // 超时控制
            Map<String, Object> values = collectFuture
                    .completeOnTimeout(Collections.emptyMap(), collectTimeoutMs, TimeUnit.MILLISECONDS)
                    .join();

            if (!values.isEmpty()) {
                // 异步数据处理
                CompletableFuture.runAsync(() -> {
                    processCollectedData(deviceId, points, values);
                }, dataProcessorPool);

                success = true;
            }

        } catch (Exception e) {
            log.error("设备 {} 批次采集失败", deviceId, e);
            connectionStates.put(deviceId, new ConnectionState(false, System.currentTimeMillis()));
        } finally {
            long executionTime = System.currentTimeMillis() - startTime;

            // 更新统计
            if (success) {
                collectionStatistics.collectionSuccess(deviceId, executionTime);
                performanceMonitor.recordBatchSuccess(deviceId, points.size(), executionTime);
                connectionStates.put(deviceId, new ConnectionState(true, System.currentTimeMillis()));
            } else {
                collectionStatistics.collectionFailed(deviceId);
                performanceMonitor.recordBatchFailure(deviceId);
            }

            // 动态调整：如果执行时间过长，适当减少后续批次大小
            if (executionTime > 100) {  // 超过100ms
                adjustBatchSize(deviceId, -10);  // 减少10%的批次大小
            } else if (executionTime < 20) {  // 执行很快
                adjustBatchSize(deviceId, 5);   // 增加5%的批次大小
            }
        }
    }

    /**
     * 自动启动所有设备
     */
    private void autoStartAllDevices() {
        try {
            log.info("开始自动启动所有设备采集...");
            startAllDevices();
            log.info("所有设备采集自动启动完成");
        } catch (Exception e) {
            log.error("自动启动采集失败", e);
        }
    }

    /**
     * 启动设备采集（核心优化方法）
     */
    public boolean startDevice(String deviceId) {
        scheduleLock.lock();
        try {
            DeviceScheduleInfo scheduleInfo = deviceScheduleInfo.get(deviceId);
            if (scheduleInfo != null && scheduleInfo.isRunning()) {
                log.warn("设备 {} 的采集已在运行", deviceId);
                return false;
            }

            // 1. 获取设备配置
            DeviceInfo deviceInfo = configManager.getDevice(deviceId);
            if (deviceInfo == null) {
                log.error("设备 {} 的配置不存在", deviceId);
                return false;
            }

            // 2. 获取设备的采集间隔（从设备配置或默认值获取）
            // 由于 collectionConfig 已被删除，这里使用默认采集间隔

            // 3. 获取数据点
            List<DataPoint> dataPoints = configManager.getDataPoints(deviceId);
            if (dataPoints.isEmpty()) {
                log.warn("设备 {} 没有配置数据点", deviceId);
                return false;
            }
            
            // 4. 初始化数据点的自适应采集配置
            if (adaptiveCollectionEnabled) {
                for (DataPoint dataPoint : dataPoints) {
                    AdaptiveCollectionUtil.initDataPointAdaptiveConfig(dataPoint);
                }
            }

            // 4. 注册设备
            try {
                collectionManager.registerDevice(deviceInfo);
            } catch (Exception e) {
                log.debug("设备 {} 已注册", deviceId);
            }

            // 5. 连接设备
            if (!connectDevice(deviceId)) {
                log.error("设备 {} 连接失败", deviceId);
                return false;
            }

            // 6. 智能分组和调度（核心优化）
            scheduleDevicePoints(deviceId, dataPoints);

            //列出执行计划
            collectionManager.rebuildReadPlans(deviceId,dataPoints);

            // 7. 更新状态
            deviceScheduleInfo.put(deviceId, new DeviceScheduleInfo(deviceId, true));
            collectionStatistics.startCollection(deviceId, dataPoints.size());

            log.info("设备 {} 采集启动成功，点数: {}", deviceId, dataPoints.size());
            return true;

        } catch (Exception e) {
            log.error("启动设备 {} 采集失败", deviceId, e);
            return false;
        } finally {
            scheduleLock.unlock();
        }
    }

    /**
     * 智能调度设备点位（核心优化算法）
     */
    private void scheduleDevicePoints(String deviceId, List<DataPoint> points) {
        // 1. 智能批量分组
        List<List<DataPoint>> batches = smartBatchGrouping(points, deviceId);

        // 2. 计算每个批次的采集间隔
        // 3. 将批次分配到不同时间片（错峰调度）
        for (int i = 0; i < batches.size(); i++) {
            List<DataPoint> batch = batches.get(i);

            // 计算批次应该分配到哪个时间片
            int timeSliceIndex = calculateOptimalTimeSlice(deviceId, i, batches.size());

            // 创建批次任务
            DeviceBatchTask batchTask = new DeviceBatchTask(deviceId, batch, timeSliceIndex);

            // 添加到对应时间片
            timeSliceTasks.get(timeSliceIndex).add(batchTask);
        }

        log.debug("设备 {} 点位调度完成，批次数: {}", deviceId, batches.size());
    }

    /**
     * 智能批量分组 - 适用于多协议采集系统
     */
    private List<List<DataPoint>> smartBatchGrouping(List<DataPoint> points, String deviceId) {
        if (points == null || points.isEmpty()) {
            return Collections.emptyList();
        }

        // 1. 首先按数据类型分组（确保相同数据类型的点在一起）
        Map<String, List<DataPoint>> dataTypeGroups = new HashMap<>();
        for (DataPoint point : points) {
            String dataType = point.getDataType() != null ? point.getDataType() : "UNKNOWN";
            dataTypeGroups.computeIfAbsent(dataType, k -> new ArrayList<>()).add(point);
        }

        List<List<DataPoint>> allBatches = new ArrayList<>();

        // 2. 对每组数据类型进行智能批量
        for (List<DataPoint> typeGroup : dataTypeGroups.values()) {
            // 2.1 按协议特定规则排序
            typeGroup.sort(this::comparePointsByAddress);

            // 2.2 智能分批
            List<List<DataPoint>> typeBatches = createSmartBatches(typeGroup, deviceId);
            allBatches.addAll(typeBatches);
        }

        // 3. 小批次合并优化（避免批次太小）
        allBatches = mergeSmallBatches(allBatches, 10); // 小于10点的批次合并

        log.debug("设备 {} 智能分组完成: {}点 -> {}批", deviceId, points.size(), allBatches.size());
        return allBatches;
    }

    /**
     * 比较两个数据点的地址（支持多协议）
     */
    private int comparePointsByAddress(DataPoint p1, DataPoint p2) {
        String addr1 = p1.getAddress();
        String addr2 = p2.getAddress();

        if (addr1 == null || addr2 == null) {
            return 0;
        }

        // 尝试按数字地址排序（适用于Modbus等）
        try {
            // 提取地址中的数字部分
            Integer num1 = extractNumberFromAddress(addr1);
            Integer num2 = extractNumberFromAddress(addr2);

            if (num1 != null && num2 != null) {
                return num1.compareTo(num2);
            }
        } catch (Exception e) {
            // 忽略，使用字符串比较
        }

        // 默认按字符串排序
        return addr1.compareTo(addr2);
    }

    /**
     * 从地址中提取数字
     */
    private Integer extractNumberFromAddress(String address) {
        if (address == null) return null;

        // 查找连续的数字
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("\\d+").matcher(address);
        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * 创建智能批次
     */
    private List<List<DataPoint>> createSmartBatches(List<DataPoint> points, String deviceId) {
        List<List<DataPoint>> batches = new ArrayList<>();

        if (points.isEmpty()) {
            return batches;
        }

        // 获取最优批量大小（根据设备性能动态调整）
        int optimalBatchSize = getOptimalBatchSize(deviceId);
        int maxBatchSize = Math.min(optimalBatchSize * 2, 100); // 最大不超过100点

        List<DataPoint> currentBatch = new ArrayList<>();
        String lastAddress = null;

        for (DataPoint point : points) {
            String currentAddress = point.getAddress();

            // 批次已满，开始新批次
            if (currentBatch.size() >= optimalBatchSize) {
                batches.add(new ArrayList<>(currentBatch));
                currentBatch.clear();
                lastAddress = null;
            }

            // 检查地址连续性（仅适用于数字地址）
            if (lastAddress != null && currentAddress != null) {
                Integer lastNum = extractNumberFromAddress(lastAddress);
                Integer currentNum = extractNumberFromAddress(currentAddress);

                // 如果地址不连续且差距较大，开始新批次
                if (lastNum != null && currentNum != null && currentNum - lastNum > 50) {
                    if (!currentBatch.isEmpty()) {
                        batches.add(new ArrayList<>(currentBatch));
                        currentBatch.clear();
                    }
                }
            }

            currentBatch.add(point);
            lastAddress = currentAddress;
        }

        // 添加最后一个批次
        if (!currentBatch.isEmpty()) {
            batches.add(currentBatch);
        }

        return batches;
    }

    /**
     * 合并小批次（提高效率）
     */
    private List<List<DataPoint>> mergeSmallBatches(List<List<DataPoint>> batches, int minBatchSize) {
        if (batches.size() <= 1) {
            return batches;
        }

        List<List<DataPoint>> mergedBatches = new ArrayList<>();
        List<DataPoint> currentBatch = new ArrayList<>();

        for (List<DataPoint> batch : batches) {
            // 如果当前批次加上新批次仍然很小，就合并
            if (currentBatch.size() + batch.size() <= minBatchSize * 2) {
                currentBatch.addAll(batch);
            } else {
                // 当前批次已经够大，保存并开始新批次
                if (!currentBatch.isEmpty()) {
                    mergedBatches.add(new ArrayList<>(currentBatch));
                }
                currentBatch = new ArrayList<>(batch);
            }
        }

        // 添加最后一个批次
        if (!currentBatch.isEmpty()) {
            mergedBatches.add(currentBatch);
        }

        // 确保没有超过最大批量大小
        List<List<DataPoint>> finalBatches = new ArrayList<>();
        for (List<DataPoint> batch : mergedBatches) {
            if (batch.size() > 100) { // 最大100点/批
                // 分割大批次
                for (int i = 0; i < batch.size(); i += 100) {
                    int end = Math.min(i + 100, batch.size());
                    finalBatches.add(new ArrayList<>(batch.subList(i, end)));
                }
            } else {
                finalBatches.add(batch);
            }
        }

        return finalBatches;
    }

    /**
     * 获取最优批量大小（基于历史数据的智能预测）
     */
    private int getOptimalBatchSize(String deviceId) {
        try {
            // 1. 优先从设备性能统计中获取历史最优值
            DevicePerformance perf = performanceMonitor.devicePerformance.get(deviceId);
            if (perf != null) {
                // 基于历史成功率和执行时间计算最优批次大小
                double successRate = perf.successfulBatches.get() / Math.max(1.0, perf.successfulBatches.get() + perf.failedBatches.get());
                long avgExecutionTime = perf.successfulBatches.get() > 0 ? 
                        perf.totalExecutionTime.get() / perf.successfulBatches.get() : 0;
                
                // 2. 智能调整策略
                int optimalSize = perf.currentBatchSize;
                
                if (successRate < 0.8) {
                    // 成功率低，减小批次大小
                    optimalSize = Math.max(10, optimalSize * 80 / 100);
                } else if (avgExecutionTime < 50 && successRate > 0.95) {
                    // 执行快且成功率高，增大批次大小
                    optimalSize = Math.min(200, optimalSize * 120 / 100);
                } else if (avgExecutionTime > 200) {
                    // 执行慢，减小批次大小
                    optimalSize = Math.max(10, optimalSize * 70 / 100);
                }
                
                // 3. 结合协议特性限制
                DeviceInfo deviceInfo = configManager.getDevice(deviceId);
                if (deviceInfo != null) {
                    String protocol = deviceInfo.getProtocolType();
                    if (protocol != null) {
                        optimalSize = Math.min(optimalSize, getProtocolMaxBatchSize(protocol));
                    }
                }
                
                return optimalSize;
            }
        } catch (Exception e) {
            log.warn("获取设备 {} 最优批次大小失败，使用默认值", deviceId, e);
        }
        
        // 默认值：根据协议类型
        return getDefaultBatchSizeByProtocol(deviceId);
    }
    
    /**
     * 根据协议类型获取默认批次大小
     */
    private int getDefaultBatchSizeByProtocol(String deviceId) {
        try {
            DeviceInfo deviceInfo = configManager.getDevice(deviceId);
            if (deviceInfo != null) {
                String protocol = deviceInfo.getProtocolType();
                if (protocol != null) {
                    return switch (protocol.toUpperCase()) {
                        case "MODBUS_TCP", "MODBUS_RTU" -> 125; // Modbus最大125
                        case "OPC_UA" -> 100; // OPC UA支持较大批量
                        case "SIEMENS_S7" -> 200; // S7协议
                        case "MQTT" -> 30; // MQTT主题不宜太多
                        case "SNMP" -> 20; // SNMP批量较小
                        default -> 50; // 默认
                    };
                }
            }
        } catch (Exception e) {
            log.warn("获取设备 {} 协议类型失败，使用默认批量大小", deviceId, e);
        }
        return 50;
    }
    
    /**
     * 获取协议最大批次大小
     */
    private int getProtocolMaxBatchSize(String protocol) {
        return switch (protocol.toUpperCase()) {
            case "MODBUS_TCP", "MODBUS_RTU" -> 125; // Modbus协议限制
            case "OPC_UA" -> 200; // OPC UA支持较大批量
            case "SIEMENS_S7" -> 300; // S7协议
            case "MQTT" -> 50; // MQTT主题数量限制
            case "SNMP" -> 30; // SNMP批量限制
            case "COAP" -> 20; // CoAP协议限制
            case "IEC104" -> 100; // IEC104协议
            default -> 200; // 默认最大
        };
    }

    /**
     * 计算最优时间片
     */
    private int calculateOptimalTimeSlice(String deviceId, int batchIndex, int totalBatches) {
        // 使用设备ID的哈希值作为基础偏移，避免所有设备在同一时间片
        int deviceHash = Math.abs(deviceId.hashCode());
        int sliceCount = TIME_SLICE_COUNT.get();
        int baseSlice = deviceHash % sliceCount;

        // 根据批次索引分配时间片（均匀分布）
        int sliceIncrement = sliceCount / Math.min(totalBatches, sliceCount);
        int sliceIndex = (baseSlice + batchIndex * sliceIncrement) % sliceCount;

        return sliceIndex;
    }

    /**
     * 连接设备
     */
    private boolean connectDevice(String deviceId) {
        try {
            collectionManager.connectDevice(deviceId);
            connectionStates.put(deviceId, new ConnectionState(true, System.currentTimeMillis()));
            configManager.getDataPointsAndAdaptiveConfig(deviceId);
            return true;
        } catch (Exception e) {
            log.error("设备 {} 连接失败", deviceId, e);
            connectionStates.put(deviceId, new ConnectionState(false, System.currentTimeMillis()));
            return false;
        }
    }

    /**
     * 重新连接设备
     */
    private boolean reconnectDevice(String deviceId) {
        try {
            collectionManager.reconnectDevice(deviceId);
            connectionStates.put(deviceId, new ConnectionState(true, System.currentTimeMillis()));
            return true;
        } catch (Exception e) {
            log.error("设备 {} 重连失败", deviceId, e);
            connectionStates.put(deviceId, new ConnectionState(false, System.currentTimeMillis()));
            return false;
        }
    }

    /**
     * 停止设备采集
     */
    public boolean stopDevice(String deviceId) {
        scheduleLock.lock();
        try {
            // 1. 从时间片任务中移除
            for (List<DeviceBatchTask> tasks : timeSliceTasks.values()) {
                tasks.removeIf(task -> task.deviceId.equals(deviceId));
            }

            // 2. 断开设备连接
            try {
                collectionManager.disconnectDevice(deviceId);
            } catch (Exception e) {
                log.warn("断开设备 {} 连接失败", deviceId, e);
            }

            // 3. 清除连接状态
            connectionStates.remove(deviceId);

            // 4. 更新调度信息
            deviceScheduleInfo.remove(deviceId);

            // 5. 停止统计
            collectionStatistics.stopCollection(deviceId);

            log.info("设备 {} 采集已停止", deviceId);
            return true;

        } catch (Exception e) {
            log.error("停止设备 {} 采集失败", deviceId, e);
            return false;
        } finally {
            scheduleLock.unlock();
        }
    }

    /**
     * 启动所有设备
     */
    public void startAllDevices() {
        List<String> deviceIds = configManager.getAllDeviceIds();
        log.info("开始启动所有设备采集，共 {} 个设备", deviceIds.size());

        int successCount = 0;
        int failCount = 0;

        for (String deviceId : deviceIds) {
            try {
                DeviceInfo deviceInfo = configManager.getDevice(deviceId);
                if (deviceInfo != null && deviceInfo.getProtocolConfig() != null ) {
                    if (startDevice(deviceId)) {
                        successCount++;
                    } else {
                        failCount++;
                    }
                }
            } catch (Exception e) {
                log.error("启动设备 {} 失败", deviceId, e);
                failCount++;
            }
        }

        log.info("所有设备启动完成，成功: {}，失败: {}", successCount, failCount);
    }

    /**
     * 停止所有设备
     */
    public void stopAllDevices() {
        List<String> runningDevices = new ArrayList<>(deviceScheduleInfo.keySet());
        log.info("开始停止所有设备采集，共 {} 个运行中设备", runningDevices.size());

        for (String deviceId : runningDevices) {
            try {
                stopDevice(deviceId);
            } catch (Exception e) {
                log.error("停止设备 {} 失败", deviceId, e);
            }
        }

        log.info("所有设备已停止");
    }

    /**
     * 重新加载所有设备
     */
    public void reloadAllDevices() {
        log.info("重新加载所有设备采集...");
        stopAllDevices();
        timeSliceScheduler.schedule(this::startAllDevices, 2, TimeUnit.SECONDS);
    }

    /**
     * 处理采集数据
     */
    private void processCollectedData(String deviceId, List<DataPoint> points,
                                      Map<String, Object> values) {
        // 这里实现数据存储、报警检查等逻辑
        for (DataPoint point : points) {
            String pointId = point.getPointId();
            Object value = values.get(pointId);

            if (value != null) {
                // TODO: 数据持久化、报警检查等
                performanceMonitor.recordDataProcessed(deviceId);

                // 如果设备之前有异常，现在恢复正常，重置自适应配置
                DevicePerformance perf = performanceMonitor.devicePerformance.get(deviceId);
                if (perf != null && perf.consecutiveFailureCount > 0) {
                    AdaptiveCollectionUtil.resetAdaptiveConfig(point);
                }

                // 自适应采集频率调整
                if (adaptiveCollectionEnabled) {
                    AdaptiveCollectionUtil.adjustCollectionFrequency(deviceId, point, value,adaptiveAdjustWindow);
                }
            }
        }
    }

    /**
     * 更新最优批量大小
     */
    private void updateOptimalBatchSize(String deviceId, int newSize) {
        // 这里可以保存到设备性能统计中
        log.debug("设备 {} 批量大小调整为: {}", deviceId, newSize);
    }

    /**
     * 调整批次大小
     */
    private void adjustBatchSize(String deviceId, int percentChange) {
        // 这里实现动态调整逻辑
        performanceMonitor.adjustBatchSize(deviceId, percentChange);
    }

    /**
     * 启动性能监控
     */
    private void startPerformanceMonitoring() {
        timeSliceScheduler.scheduleAtFixedRate(performanceMonitor::logStatistics, 60, 60, TimeUnit.SECONDS);  // 每分钟输出一次统计
    }
    
    /**
     * 启动动态时间片调整任务
     */
    private void startDynamicTimeSliceAdjustment() {
        timeSliceScheduler.scheduleAtFixedRate(this::adjustTimeSlicesDynamically, dynamicAdjustInterval, dynamicAdjustInterval, TimeUnit.MILLISECONDS);
        log.info("动态时间片调整已启动，调整间隔: {}ms", dynamicAdjustInterval);
    }
    
    /**
     * 动态调整时间片数量和间隔
     */
    private void adjustTimeSlicesDynamically() {
        try {
            // 1. 获取当前系统负载
            double cpuLoad = getSystemCpuLoad();
            int activeDevices = deviceScheduleInfo.size();
            long totalTasks = timeSliceTasks.values().stream().mapToInt(List::size).sum();
            
            // 2. 计算新的时间片数量
            int newSliceCount = calculateOptimalSliceCount(activeDevices, totalTasks, cpuLoad);
            
            // 3. 根据最新执行情况和超时标记进行调节
            long avgExecution = performanceMonitor.getAverageTimeSliceExecution();
            boolean timeoutDetected = performanceMonitor.consumeTimeSliceTimeout();
            int tunedInterval = timeSliceTuner != null
                    ? timeSliceTuner.adjustInterval(TIME_SLICE_INTERVAL.get(), avgExecution, timeoutDetected)
                    : TIME_SLICE_INTERVAL.get();
            
            // 4. 更新时间片配置
            updateTimeSliceConfig(newSliceCount, tunedInterval);

            log.info("动态调整时间片: 设备数={}, 任务数={}, CPU负载={}, 平均执行={}ms, 有超时={}, 调整后片数={}, 片间隔={}ms, 调整模式={}",
                    activeDevices,
                    totalTasks,
                    String.format("%.2f", cpuLoad),
                    avgExecution,
                    timeoutDetected,
                    newSliceCount,
                    tunedInterval,
                    timeSliceTuner != null ? timeSliceTuner.getMode() : "UNKNOWN");
        } catch (Exception e) {
            log.error("动态调整时间片失败", e);
        }
    }
    
    /**
     * 计算最优时间片数量
     */
    private int calculateOptimalSliceCount(int activeDevices, long totalTasks, double cpuLoad) {
        // 基础片数：根据设备数和任务数计算
        int baseSlices = Math.max(1, Math.min(activeDevices / 5 + 1, maxTimeSliceCount));
        
        // 根据CPU负载调整
        if (cpuLoad > 0.8) {
            // 高负载时增加片数，降低每片任务数
            baseSlices = Math.min(maxTimeSliceCount, baseSlices + 2);
        } else if (cpuLoad < 0.3) {
            // 低负载时减少片数，增加每片任务数
            baseSlices = Math.max(2, baseSlices - 1);
        }
        
        return baseSlices;
    }
    
    /**
     * 计算最优时间片间隔
     */
    /**
     * 更新时间片配置
     */
    private void updateTimeSliceConfig(int newSliceCount, int newSliceInterval) {
        int oldSliceCount = TIME_SLICE_COUNT.get();
        
        // 更新时间片数量
        if (newSliceCount != oldSliceCount) {
            TIME_SLICE_COUNT.set(newSliceCount);
            
            // 重新初始化时间片任务队列
            timeSliceTasks.clear();
            for (int i = 0; i < newSliceCount; i++) {
                timeSliceTasks.put(i, new CopyOnWriteArrayList<>());
            }
            
            // 重新调度所有设备
            rescheduleAllDevices();
        }
        
        // 更新时间片间隔
        TIME_SLICE_INTERVAL.set(newSliceInterval);
    }
    
    /**
     * 重新调度所有设备
     */
    private void rescheduleAllDevices() {
        List<String> deviceIds = new ArrayList<>(deviceScheduleInfo.keySet());
        for (String deviceId : deviceIds) {
            try {
                stopDevice(deviceId);
                startDevice(deviceId);
            } catch (Exception e) {
                log.error("重新调度设备 {} 失败", deviceId, e);
            }
        }
    }
    
    /**
     * 获取系统CPU负载
     */
    private double getSystemCpuLoad() {
        // 简化实现，返回一个基于活跃线程数的估算值
        int activeThreads = asyncCollectorPool.getActiveCount() + dataProcessorPool.getActiveCount();
        int maxThreads = asyncCollectorPool.getMaximumPoolSize() + dataProcessorPool.getMaximumPoolSize();
        return Math.min(1.0, (double) activeThreads / maxThreads);
    }

    /**
     * 关闭执行器
     */
    private void shutdownExecutor(String name, ExecutorService executor) {
        if (executor != null && !executor.isShutdown()) {
            try {
                executor.shutdown();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            log.info("{} 已关闭", name);
        }
    }

    /**
     * 获取设备调度状态
     */
    public Map<String, Object> getDeviceScheduleStatus(String deviceId) {
        Map<String, Object> status = new HashMap<>();
        status.put("deviceId", deviceId);

        DeviceScheduleInfo info = deviceScheduleInfo.get(deviceId);
        status.put("isRunning", info != null && info.isRunning());
        status.put("connectionState", connectionStates.get(deviceId));
        status.put("statistics", collectionStatistics.getDeviceStatistics(deviceId));
        status.put("performance", performanceMonitor.getDevicePerformance(deviceId));

        return status;
    }

    /**
     * 获取运行中的设备列表
     */
    public List<String> getRunningDevices() {
        return deviceScheduleInfo.entrySet().stream()
                .filter(entry -> entry.getValue().isRunning())
                .map(Map.Entry::getKey)
                .toList();
    }

    /**
     * 检查设备是否在运行
     */
    public boolean isDeviceRunning(String deviceId) {
        DeviceScheduleInfo info = deviceScheduleInfo.get(deviceId);
        return info != null && info.isRunning();
    }

    /**
     * 配置更新事件监听
     */
    @EventListener
    public void handleConfigUpdate(ConfigUpdateEvent event) {
        String deviceId = event.getDeviceId();
        if (deviceId != null && isDeviceRunning(deviceId)) {
            log.info("设备 {} 配置更新，重新启动采集", deviceId);
            timeSliceScheduler.schedule(() -> {
                stopDevice(deviceId);
                startDevice(deviceId);
                configManager.getDataPointsAndAdaptiveConfig(deviceId);
            }, 1, TimeUnit.SECONDS);
        }
    }

    // ================== 内部数据结构 ==================

    /**
     * 设备调度信息
     */
    private static class DeviceScheduleInfo {
        String deviceId;
        boolean running;
        long startTime;

        DeviceScheduleInfo(String deviceId, boolean running) {
            this.deviceId = deviceId;
            this.running = running;
            this.startTime = System.currentTimeMillis();
        }

        boolean isRunning() {
            return running;
        }
    }

    /**
     * 设备批次任务
     */
    private static class DeviceBatchTask {
        String deviceId;
        List<DataPoint> points;
        int timeSliceIndex;
        long lastExecutionTime;
        AtomicInteger failureCount = new AtomicInteger(0);

        DeviceBatchTask(String deviceId, List<DataPoint> points, int timeSliceIndex) {
            this.deviceId = deviceId;
            this.points = points;
            this.timeSliceIndex = timeSliceIndex;
        }

        boolean shouldSkip() {
            // 如果连续失败多次，暂时跳过
            return failureCount.get() > 3;
        }

        void recordFailure() {
            failureCount.incrementAndGet();
        }

        void recordSuccess() {
            failureCount.set(0);
        }
    }

    /**
     * 连接状态
     */
    private static class ConnectionState {
        boolean connected;
        long lastCheckTime;

        ConnectionState(boolean connected, long lastCheckTime) {
            this.connected = connected;
            this.lastCheckTime = lastCheckTime;
        }

        boolean isConnected() {
            return connected;
        }
    }

    /**
     * 性能监控器
     */
    private class PerformanceMonitor {
        private final Map<String, DevicePerformance> devicePerformance = new ConcurrentHashMap<>();
        private final AtomicLong totalProcessedPoints = new AtomicLong(0);
        private final AtomicLong totalSuccessfulBatches = new AtomicLong(0);
        private final AtomicLong totalFailedBatches = new AtomicLong(0);
        private final Map<Integer, Long> timeSliceExecutionTimes = new ConcurrentHashMap<>();
        
        // 系统资源监控
        private final AtomicLong peakMemoryUsage = new AtomicLong(0);
        private final AtomicLong cpuUsage = new AtomicLong(0);
        
        // 瓶颈分析
        private final Map<String, Long> slowestDevices = new ConcurrentHashMap<>(); // 最慢的设备列表
        private final Map<Integer, Long> overloadedSlices = new ConcurrentHashMap<>(); // 过载的时间片
        private final AtomicBoolean recentTimeSliceTimeout = new AtomicBoolean(false);
        
        // 统计周期
        private static final long STATISTICS_CYCLE = 60000; // 60秒统计周期
        private long lastStatisticsTime = System.currentTimeMillis();

        void recordTimeSliceExecution(int sliceIndex, long executionTime) {
            timeSliceExecutionTimes.put(sliceIndex, executionTime);
            
            // 检查时间片是否过载（执行时间超过时间片间隔）
            if (executionTime > TIME_SLICE_INTERVAL.get()) {
                overloadedSlices.put(sliceIndex, executionTime);
                recentTimeSliceTimeout.set(true);
            }
        }

        void recordBatchSuccess(String deviceId, int pointCount, long executionTime) {
            totalProcessedPoints.addAndGet(pointCount);
            totalSuccessfulBatches.incrementAndGet();

            DevicePerformance perf = devicePerformance.computeIfAbsent(
                    deviceId, k -> new DevicePerformance(deviceId)
            );
            perf.recordSuccess(pointCount, executionTime);
            
            // 记录最慢设备（执行时间超过200ms）
            if (executionTime > 200) {
                slowestDevices.put(deviceId, executionTime);
            }
        }

        void recordBatchFailure(String deviceId) {
            totalFailedBatches.incrementAndGet();

            DevicePerformance perf = devicePerformance.computeIfAbsent(
                    deviceId, k -> new DevicePerformance(deviceId)
            );
            perf.recordFailure();
        }

        void recordDataProcessed(String deviceId) {
            DevicePerformance perf = devicePerformance.get(deviceId);
            if (perf != null) {
                perf.recordDataProcessed();
            }
        }

        void adjustBatchSize(String deviceId, int percentChange) {
            DevicePerformance perf = devicePerformance.get(deviceId);
            if (perf != null) {
                perf.adjustBatchSize(percentChange);
            }
        }

        void logStatistics() {
            long currentTime = System.currentTimeMillis();
            long elapsedTime = currentTime - lastStatisticsTime;
            lastStatisticsTime = currentTime;
            
            long totalPoints = totalProcessedPoints.getAndSet(0);
            long successfulBatches = totalSuccessfulBatches.getAndSet(0);
            long failedBatches = totalFailedBatches.getAndSet(0);

            double pointsPerSecond = elapsedTime > 0 ? totalPoints / (elapsedTime / 1000.0) : 0;
            double batchSuccessRate = successfulBatches + failedBatches > 0 ? 
                    successfulBatches * 100.0 / (successfulBatches + failedBatches) : 0;

            log.info("性能统计 - 处理点总数：{}, 平均每秒点数: {}/秒, 成功率: {}%, 活跃设备: {}",
                    totalPoints,
                    String.format("%.2f", pointsPerSecond),
                    String.format("%.2f", batchSuccessRate),
                    devicePerformance.size()
            );

            // 输出时间片执行情况
            StringBuilder sliceInfo = new StringBuilder("时间片执行时间: ");
            for (Map.Entry<Integer, Long> entry : timeSliceExecutionTimes.entrySet()) {
                sliceInfo.append(String.format("[%d:%dms]", entry.getKey(), entry.getValue()));
                if (entry.getValue() > TIME_SLICE_INTERVAL.get()) {
                    sliceInfo.append("(OVERLOAD)");
                }
                sliceInfo.append(", ");
            }
            if (!timeSliceExecutionTimes.isEmpty()) {
                sliceInfo.setLength(sliceInfo.length() - 2); // 移除最后一个逗号和空格
            }
            log.debug(sliceInfo.toString());
            
            // 输出瓶颈分析
            analyzeBottlenecks();
            
            // 输出设备健康状态
            reportDeviceHealth();
        }
        
        /**
         * 瓶颈分析
         */
        private void analyzeBottlenecks() {
            // 1. 检查慢设备
            if (!slowestDevices.isEmpty()) {
                // 按执行时间排序，取前5个最慢设备
                List<Map.Entry<String, Long>> sortedSlowest = slowestDevices.entrySet().stream()
                        .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                        .limit(5)
                        .toList();
                
                StringBuilder slowDeviceInfo = new StringBuilder("慢速设备（执行时间>200ms）: ");
                for (Map.Entry<String, Long> entry : sortedSlowest) {
                    slowDeviceInfo.append(String.format("%s:%dms, ", entry.getKey(), entry.getValue()));
                }
                slowDeviceInfo.setLength(slowDeviceInfo.length() - 2);
                log.warn(slowDeviceInfo.toString());
                
                // 清空慢速设备列表，准备下一轮统计
                slowestDevices.clear();
            }
            
            // 2. 检查过载时间片
            if (!overloadedSlices.isEmpty()) {
                StringBuilder overloadInfo = new StringBuilder("过载时间片（执行时间>片间隔）: ");
                for (Map.Entry<Integer, Long> entry : overloadedSlices.entrySet()) {
                    overloadInfo.append(String.format("%d:%dms, ", entry.getKey(), entry.getValue()));
                }
                overloadInfo.setLength(overloadInfo.length() - 2);
                log.warn(overloadInfo.toString());
                
                // 清空过载时间片列表
                overloadedSlices.clear();
            }
            
            // 3. 检查系统资源
            Runtime runtime = Runtime.getRuntime();
            long currentMemory = runtime.totalMemory() - runtime.freeMemory();
            if (currentMemory > peakMemoryUsage.get()) {
                peakMemoryUsage.set(currentMemory);
            }
            
            log.debug("系统资源: 当前内存占用={}MB, 峰值内存={}MB, 可用处理器数={}",
                    currentMemory / (1024 * 1024),
                    peakMemoryUsage.get() / (1024 * 1024),
                    Runtime.getRuntime().availableProcessors());
        }
        
        /**
         * 设备健康报告
         */
        private void reportDeviceHealth() {
            // 统计不同健康状态的设备数量
            long healthyDevices = 0;
            long warningDevices = 0;
            long criticalDevices = 0;
            
            for (DevicePerformance perf : devicePerformance.values()) {
                double healthScore = perf.calculateHealthScore();
                String risk = perf.predictFailureRisk();
                
                if (healthScore > 80 && "NONE".equals(risk)) {
                    healthyDevices++;
                } else if (healthScore > 60 || "LOW".equals(risk)) {
                    warningDevices++;
                } else {
                    criticalDevices++;
                }
                
                // 记录高风险设备
                if ("HIGH".equals(risk) || healthScore < 50) {
                    log.warn("设备 {} 健康度低: 健康分={}%, 故障风险={}, 连续失败={}次",
                            perf.deviceId, 
                            String.format("%.1f", healthScore),
                            risk,
                            perf.consecutiveFailureCount);
                }
            }
            
            log.info("设备健康统计: 健康={}, 警告={}, 危险={}", healthyDevices, warningDevices, criticalDevices);
        }

        Map<String, Object> getDevicePerformance(String deviceId) {
            DevicePerformance perf = devicePerformance.get(deviceId);
            if (perf != null) {
                return perf.getStatistics();
            }
            return Collections.emptyMap();
        }

        long getAverageTimeSliceExecution() {
            if (timeSliceExecutionTimes.isEmpty()) {
                return 0;
            }
            long sum = 0;
            int count = 0;
            for (Long value : timeSliceExecutionTimes.values()) {
                if (value != null && value > 0) {
                    sum += value;
                    count++;
                }
            }
            return count == 0 ? 0 : sum / count;
        }

        boolean consumeTimeSliceTimeout() {
            return recentTimeSliceTimeout.getAndSet(false);
        }
    }

    /**
     * 设备性能统计
     */
    private static class DevicePerformance {
        String deviceId;
        AtomicLong totalPoints = new AtomicLong(0);
        AtomicLong successfulBatches = new AtomicLong(0);
        AtomicLong failedBatches = new AtomicLong(0);
        AtomicLong totalExecutionTime = new AtomicLong(0);
        int currentBatchSize = 30;
        long lastAdjustTime = System.currentTimeMillis();
        
        // 设备健康度相关
        double healthScore = 100.0; // 健康度分数，100为满分
        long lastHealthCheckTime = System.currentTimeMillis();
        int consecutiveFailureCount = 0; // 连续失败次数
        long lastSuccessTime = System.currentTimeMillis(); // 最后成功时间
        
        // 响应时间趋势
        List<Long> recentResponseTimes = new ArrayList<>(); // 最近10次响应时间
        static final int MAX_RESPONSE_TIME_HISTORY = 10;
        
        // 数据变化率
        Map<String, Double> pointChangeRates = new ConcurrentHashMap<>(); // 数据点变化率统计
        Map<String, Object> lastValues = new ConcurrentHashMap<>(); // 数据点最后值

        DevicePerformance(String deviceId) {
            this.deviceId = deviceId;
        }

        void recordSuccess(int pointCount, long executionTime) {
            totalPoints.addAndGet(pointCount);
            successfulBatches.incrementAndGet();
            totalExecutionTime.addAndGet(executionTime);
            
            // 更新响应时间历史
            updateResponseTimeHistory(executionTime);
            
            // 重置连续失败计数
            consecutiveFailureCount = 0;
            lastSuccessTime = System.currentTimeMillis();
        }

        void recordFailure() {
            failedBatches.incrementAndGet();
            consecutiveFailureCount++;
            // 更新响应时间历史为-1表示失败
            updateResponseTimeHistory(-1);
        }

        void recordDataProcessed() {
            // 可记录数据处理相关统计
        }
        
        /**
         * 更新响应时间历史
         */
        void updateResponseTimeHistory(long executionTime) {
            synchronized (recentResponseTimes) {
                recentResponseTimes.add(executionTime);
                if (recentResponseTimes.size() > MAX_RESPONSE_TIME_HISTORY) {
                    recentResponseTimes.remove(0);
                }
            }
        }
        
        /**
         * 计算设备健康度分数
         */
        double calculateHealthScore() {
            double score = 100.0;
            
            // 1. 成功率影响（50%权重）
            double successRate = successfulBatches.get() / Math.max(1.0, successfulBatches.get() + failedBatches.get());
            score *= (successRate * 0.5 + 0.5);
            
            // 2. 连续失败次数影响（20%权重）
            if (consecutiveFailureCount > 0) {
                double failurePenalty = Math.max(0.1, 1.0 - consecutiveFailureCount * 0.1);
                score *= (failurePenalty * 0.2 + 0.8);
            }
            
            // 3. 响应时间趋势影响（20%权重）
            double avgResponseTime = getAverageResponseTime();
            if (avgResponseTime > 200) { // 响应时间超过200ms
                double responsePenalty = Math.max(0.5, 1.0 - (avgResponseTime - 200) / 1000.0);
                score *= (responsePenalty * 0.2 + 0.8);
            }
            
            // 4. 最近活动影响（10%权重）
            long inactiveTime = System.currentTimeMillis() - lastSuccessTime;
            if (inactiveTime > 300000) { // 超过5分钟无成功
                double inactivePenalty = Math.max(0.5, 1.0 - (inactiveTime - 300000) / 300000.0);
                score *= (inactivePenalty * 0.1 + 0.9);
            }
            
            // 限制分数范围
            healthScore = Math.max(0.0, Math.min(100.0, score));
            lastHealthCheckTime = System.currentTimeMillis();
            
            return healthScore;
        }
        
        /**
         * 获取平均响应时间
         */
        long getAverageResponseTime() {
            synchronized (recentResponseTimes) {
                if (recentResponseTimes.isEmpty()) {
                    return 0;
                }
                return (long) recentResponseTimes.stream()
                        .filter(time -> time > 0)
                        .mapToLong(Long::longValue)
                        .average()
                        .orElse(0.0);
            }
        }
        
        /**
         * 预测设备故障风险
         */
        String predictFailureRisk() {
            if (consecutiveFailureCount >= 3) {
                return "HIGH";
            } else if (calculateHealthScore() < 60) {
                return "MEDIUM";
            } else if (getAverageResponseTime() > 300) {
                return "LOW";
            } else {
                return "NONE";
            }
        }

        void adjustBatchSize(int percentChange) {
            long now = System.currentTimeMillis();
            if (now - lastAdjustTime < 10000) {  // 10秒内不重复调整
                return;
            }

            int oldSize = currentBatchSize;
            currentBatchSize = Math.max(10, Math.min(100,
                    currentBatchSize * (100 + percentChange) / 100));

            if (oldSize != currentBatchSize) {
                log.debug("设备 {} 批量大小从 {} 调整为 {}", deviceId, oldSize, currentBatchSize);
                lastAdjustTime = now;
            }
        }

        Map<String, Object> getStatistics() {
            Map<String, Object> stats = new HashMap<>();
            stats.put("deviceId", deviceId);
            stats.put("totalPoints", totalPoints.get());
            stats.put("successfulBatches", successfulBatches.get());
            stats.put("failedBatches", failedBatches.get());
            stats.put("averageBatchTime", successfulBatches.get() > 0 ?
                    totalExecutionTime.get() / successfulBatches.get() : 0);
            stats.put("currentBatchSize", currentBatchSize);
            stats.put("successRate", (successfulBatches.get() + failedBatches.get()) > 0 ?
                    successfulBatches.get() * 100.0 / (successfulBatches.get() + failedBatches.get()) : 0);
            stats.put("healthScore", calculateHealthScore());
            stats.put("failureRisk", predictFailureRisk());
            stats.put("consecutiveFailures", consecutiveFailureCount);
            stats.put("averageResponseTime", getAverageResponseTime());
            stats.put("recentResponseTimes", recentResponseTimes);
            return stats;
        }
    }

    /**
     * 命名线程工厂
     */
    private static class NamedThreadFactory implements ThreadFactory {
        private final String namePrefix;
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        NamedThreadFactory(String namePrefix) {
            this.namePrefix = namePrefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, namePrefix + "-" + threadNumber.getAndIncrement());
            thread.setPriority(Thread.NORM_PRIORITY);
            thread.setDaemon(true);
            return thread;
        }
    }
}
