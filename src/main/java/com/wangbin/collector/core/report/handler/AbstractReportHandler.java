package com.wangbin.collector.core.report.handler;

import com.wangbin.collector.core.report.model.ReportConfig;
import com.wangbin.collector.core.report.model.ReportData;
import com.wangbin.collector.core.report.model.ReportResult;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 抽象报告处理器
 *
 * 职责：
 * 1. 提供报告处理器的通用实现
 * 2. 管理处理器配置
 * 3. 记录上报统计信息
 * 4. 处理异常和重试逻辑
 */
@Slf4j
@Getter
public abstract class AbstractReportHandler implements ReportHandler {

    // 处理器名称
    protected String name;

    // 协议类型
    protected String protocol;

    // 处理器描述
    protected String description;

    // 是否启用
    protected boolean enabled = true;

    // 处理器配置
    protected Map<String, Object> config = new HashMap<>();

    // 上报统计
    protected final AtomicLong totalReportCount = new AtomicLong(0);
    protected final AtomicLong successReportCount = new AtomicLong(0);
    protected final AtomicLong failureReportCount = new AtomicLong(0);
    protected final AtomicLong totalCostTime = new AtomicLong(0);
    protected final AtomicLong totalDataSize = new AtomicLong(0);

    // 最大重试次数
    protected int maxRetryCount = 3;

    // 重试间隔（毫秒）
    protected long retryInterval = 1000;

    // 连接超时时间（毫秒）
    protected long connectTimeout = 5000;

    // 读取超时时间（毫秒）
    protected Integer readTimeout = 10000;

    /**
     * 构造函数
     *
     * @param name 处理器名称
     * @param protocol 协议类型
     * @param description 处理器描述
     */
    protected AbstractReportHandler(String name, String protocol, String description) {
        this.name = name;
        this.protocol = protocol;
        this.description = description;
    }

    /**
     * 初始化处理器
     *
     * 执行步骤：
     * 1. 加载默认配置
     * 2. 验证配置有效性
     * 3. 初始化连接资源
     * 4. 启动心跳任务
     *
     * @throws Exception 初始化失败时抛出异常
     */
    @Override
    public void init() throws Exception {
        log.info("初始化报告处理器：{} [{}]", name, protocol);

        try {
            // 1. 加载默认配置
            loadDefaultConfig();

            // 2. 验证配置有效性
            validateConfig();

            // 3. 执行具体实现初始化
            doInit();

            log.info("报告处理器初始化完成：{} [{}]", name, protocol);
        } catch (Exception e) {
            log.error("报告处理器初始化失败：{} [{}]", name, protocol, e);
            throw e;
        }
    }

    /**
     * 上报单个数据
     *
     * 执行步骤：
     * 1. 验证数据和配置
     * 2. 数据编码和打包
     * 3. 建立连接并发送数据
     * 4. 处理响应
     * 5. 记录统计
     *
     * @param data 上报数据
     * @param config 上报配置
     * @return 上报结果
     * @throws Exception 上报失败时抛出异常
     */
    @Override
    public ReportResult report(ReportData data, ReportConfig config) throws Exception {
        if (!enabled) {
            return ReportResult.error(data.getPointCode(), "处理器已禁用", config.getTargetId());
        }

        if (data == null) {
            throw new IllegalArgumentException("上报数据不能为null");
        }

        if (config == null) {
            throw new IllegalArgumentException("上报配置不能为null");
        }

        long startTime = System.currentTimeMillis();
        ReportResult result = null;
        int retryCount = 0;

        try {
            // 验证数据和配置
            validateData(data);
            validateConfig(config);

            // 数据预处理
            ReportData processedData = preProcessData(data, config);

            // 重试逻辑
            while (retryCount <= maxRetryCount) {
                try {
                    // 执行具体上报逻辑
                    result = doReport(processedData, config);

                    // 记录统计
                    recordReport(result.isSuccess(),
                            System.currentTimeMillis() - startTime,
                            processedData.getDataSize());

                    if (result.isSuccess()) {
                        log.debug("数据上报成功：{} -> {} [{}]",
                                data.getPointCode(), config.getTargetId(), protocol);
                        break;
                    } else {
                        log.warn("数据上报失败：{} -> {} [{}]，错误：{}",
                                data.getPointCode(), config.getTargetId(), protocol,
                                result.getErrorMessage());

                        if (retryCount < maxRetryCount) {
                            retryCount++;
                            log.debug("准备重试上报：{}，重试次数：{}", data.getPointCode(), retryCount);
                            Thread.sleep(retryInterval);
                        }
                    }
                } catch (Exception e) {
                    log.error("上报异常：{} -> {} [{}]",
                            data.getPointCode(), config.getTargetId(), protocol, e);

                    if (retryCount < maxRetryCount) {
                        retryCount++;
                        log.debug("准备重试上报（异常）：{}，重试次数：{}", data.getPointCode(), retryCount);
                        Thread.sleep(retryInterval);
                    } else {
                        throw e;
                    }
                }
            }

            // 如果没有成功结果，创建错误结果
            if (result == null) {
                result = ReportResult.error(data.getPointCode(),
                        "上报失败，重试" + maxRetryCount + "次后仍然失败", config.getTargetId());
                recordReport(false, System.currentTimeMillis() - startTime, 0);
            }

            // 后处理
            postProcessReport(result, data, config);

            return result;

        } catch (Exception e) {
            log.error("上报过程异常：{} -> {} [{}]",
                    data.getPointCode(), config.getTargetId(), protocol, e);

            result = ReportResult.error(data.getPointCode(),
                    "上报异常：" + e.getMessage(), config.getTargetId());
            recordReport(false, System.currentTimeMillis() - startTime, 0);

            return result;
        }
    }

    /**
     * 批量上报数据
     *
     * 执行步骤：
     * 1. 验证数据和配置
     * 2. 数据分组和打包
     * 3. 批量发送数据
     * 4. 处理批量响应
     * 5. 记录批量统计
     *
     * @param dataList 数据列表
     * @param config 上报配置
     * @return 上报结果列表
     * @throws Exception 上报失败时抛出异常
     */
    @Override
    public List<ReportResult> batchReport(List<ReportData> dataList, ReportConfig config) throws Exception {
        if (!enabled) {
            List<ReportResult> results = new ArrayList<>();
            for (ReportData data : dataList) {
                results.add(ReportResult.error(data.getPointCode(),
                        "处理器已禁用", config.getTargetId()));
            }
            return results;
        }

        if (dataList == null || dataList.isEmpty()) {
            return Collections.emptyList();
        }

        if (config == null) {
            throw new IllegalArgumentException("上报配置不能为null");
        }

        long startTime = System.currentTimeMillis();
        List<ReportResult> results = new ArrayList<>(dataList.size());

        try {
            // 验证配置
            validateConfig(config);

            // 数据预处理
            List<ReportData> processedDataList = new ArrayList<>(dataList.size());
            for (ReportData data : dataList) {
                try {
                    validateData(data);
                    processedDataList.add(preProcessData(data, config));
                } catch (Exception e) {
                    log.warn("数据预处理失败，跳过：{}", data.getPointCode(), e);
                    results.add(ReportResult.error(data.getPointCode(),
                            "数据预处理失败：" + e.getMessage(), config.getTargetId()));
                }
            }

            if (processedDataList.isEmpty()) {
                return results;
            }

            // 执行批量上报
            List<ReportResult> batchResults = doBatchReport(processedDataList, config);
            results.addAll(batchResults);

            // 记录统计
            int successCount = 0;
            int totalSize = 0;
            for (int i = 0; i < batchResults.size(); i++) {
                ReportResult result = batchResults.get(i);
                if (result.isSuccess()) {
                    successCount++;
                }
                if (i < processedDataList.size()) {
                    totalSize += processedDataList.get(i).getDataSize();
                }
            }

            recordBatchReport(successCount, batchResults.size() - successCount,
                    batchResults.size(), totalSize,
                    System.currentTimeMillis() - startTime);

            log.debug("批量上报完成：{}，成功：{}，失败：{}，总计：{}，耗时：{}ms",
                    config.getTargetId(), successCount,
                    batchResults.size() - successCount, batchResults.size(),
                    System.currentTimeMillis() - startTime);

            return results;

        } catch (Exception e) {
            log.error("批量上报异常：{} [{}]", config.getTargetId(), protocol, e);

            // 为所有数据创建错误结果
            for (ReportData data : dataList) {
                results.add(ReportResult.error(data.getPointCode(),
                        "批量上报异常：" + e.getMessage(), config.getTargetId()));
            }

            recordBatchReport(0, dataList.size(), dataList.size(), 0,
                    System.currentTimeMillis() - startTime);

            return results;
        }
    }

    /**
     * 处理配置更新
     *
     * @param config 新的配置
     * @throws Exception 处理失败时抛出异常
     */
    @Override
    public void onConfigUpdate(ReportConfig config) throws Exception {
        log.info("处理器配置更新：{} -> {} [{}]", name, config.getTargetId(), protocol);

        try {
            // 验证新配置
            validateConfig(config);

            // 更新处理器配置
            updateHandlerConfig(config);

            // 执行具体实现配置更新
            doConfigUpdate(config);

            log.info("处理器配置更新完成：{} -> {} [{}]", name, config.getTargetId(), protocol);
        } catch (Exception e) {
            log.error("处理器配置更新失败：{} -> {} [{}]", name, config.getTargetId(), protocol, e);
            throw e;
        }
    }

    /**
     * 处理配置删除
     *
     * @param config 被删除的配置
     * @throws Exception 处理失败时抛出异常
     */
    @Override
    public void onConfigRemove(ReportConfig config) throws Exception {
        log.info("处理器配置删除：{} -> {} [{}]", name, config.getTargetId(), protocol);

        try {
            // 执行具体实现配置删除
            doConfigRemove(config);

            log.info("处理器配置删除完成：{} -> {} [{}]", name, config.getTargetId(), protocol);
        } catch (Exception e) {
            log.error("处理器配置删除失败：{} -> {} [{}]", name, config.getTargetId(), protocol, e);
            throw e;
        }
    }

    /**
     * 获取处理器状态
     *
     * @return 状态信息映射
     */
    @Override
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();

        status.put("name", name);
        status.put("protocol", protocol);
        status.put("description", description);
        status.put("enabled", enabled);
        status.put("maxRetryCount", maxRetryCount);
        status.put("retryInterval", retryInterval);
        status.put("connectTimeout", connectTimeout);
        status.put("readTimeout", readTimeout);

        // 添加具体实现的状态信息
        Map<String, Object> implStatus = getImplementationStatus();
        if (implStatus != null) {
            status.putAll(implStatus);
        }

        return status;
    }

    /**
     * 获取处理器统计信息
     *
     * @return 统计信息映射
     */
    @Override
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();

        long total = totalReportCount.get();
        long success = successReportCount.get();
        long failure = failureReportCount.get();
        long totalTime = totalCostTime.get();
        long totalSize = totalDataSize.get();

        stats.put("totalReportCount", total);
        stats.put("successReportCount", success);
        stats.put("failureReportCount", failure);
        stats.put("totalCostTime", totalTime);
        stats.put("totalDataSize", totalSize);

        // 计算成功率
        double successRate = total > 0 ? (double) success / total * 100 : 0;
        stats.put("successRate", String.format("%.2f%%", successRate));

        // 计算平均耗时
        long avgCostTime = total > 0 ? totalTime / total : 0;
        stats.put("avgCostTime", avgCostTime);

        // 计算平均数据大小
        long avgDataSize = total > 0 ? totalSize / total : 0;
        stats.put("avgDataSize", avgDataSize);

        // 添加具体实现的统计信息
        Map<String, Object> implStats = getImplementationStatistics();
        if (implStats != null) {
            stats.putAll(implStats);
        }

        return stats;
    }

    /**
     * 重置统计信息
     */
    @Override
    public void resetStatistics() {
        totalReportCount.set(0);
        successReportCount.set(0);
        failureReportCount.set(0);
        totalCostTime.set(0);
        totalDataSize.set(0);

        log.info("重置处理器统计信息：{} [{}]", name, protocol);
    }

    /**
     * 销毁处理器
     *
     * 执行步骤：
     * 1. 关闭所有连接
     * 2. 停止所有任务
     * 3. 清理所有资源
     *
     * @throws Exception 销毁失败时抛出异常
     */
    @Override
    public void destroy() throws Exception {
        log.info("销毁报告处理器：{} [{}]", name, protocol);

        try {
            // 执行具体实现销毁
            doDestroy();

            // 清理配置
            config.clear();

            log.info("报告处理器销毁完成：{} [{}]", name, protocol);
        } catch (Exception e) {
            log.error("报告处理器销毁失败：{} [{}]", name, protocol, e);
            throw e;
        }
    }

    /**
     * 启用处理器
     */
    public void enable() {
        this.enabled = true;
        log.info("启用报告处理器：{} [{}]", name, protocol);
    }

    /**
     * 禁用处理器
     */
    public void disable() {
        this.enabled = false;
        log.info("禁用报告处理器：{} [{}]", name, protocol);
    }

    // =============== 抽象方法 ===============

    /**
     * 执行具体初始化逻辑
     *
     * @throws Exception 初始化失败时抛出异常
     */
    protected abstract void doInit() throws Exception;

    /**
     * 执行具体上报逻辑
     *
     * @param data 上报数据
     * @param config 上报配置
     * @return 上报结果
     * @throws Exception 上报失败时抛出异常
     */
    protected abstract ReportResult doReport(ReportData data, ReportConfig config) throws Exception;

    /**
     * 执行具体批量上报逻辑
     *
     * @param dataList 数据列表
     * @param config 上报配置
     * @return 上报结果列表
     * @throws Exception 上报失败时抛出异常
     */
    protected abstract List<ReportResult> doBatchReport(List<ReportData> dataList, ReportConfig config) throws Exception;

    /**
     * 执行具体配置更新逻辑
     *
     * @param config 新的配置
     * @throws Exception 处理失败时抛出异常
     */
    protected abstract void doConfigUpdate(ReportConfig config) throws Exception;

    /**
     * 执行具体配置删除逻辑
     *
     * @param config 被删除的配置
     * @throws Exception 处理失败时抛出异常
     */
    protected abstract void doConfigRemove(ReportConfig config) throws Exception;

    /**
     * 执行具体销毁逻辑
     *
     * @throws Exception 销毁失败时抛出异常
     */
    protected abstract void doDestroy() throws Exception;

    /**
     * 获取具体实现的状态信息
     *
     * @return 状态信息映射
     */
    protected abstract Map<String, Object> getImplementationStatus();

    /**
     * 获取具体实现的统计信息
     *
     * @return 统计信息映射
     */
    protected abstract Map<String, Object> getImplementationStatistics();

    // =============== 辅助方法 ===============

    /**
     * 加载默认配置
     */
    protected void loadDefaultConfig() {
        // 可以从配置文件或数据库加载默认配置
        config.put("maxRetryCount", maxRetryCount);
        config.put("retryInterval", retryInterval);
        config.put("connectTimeout", connectTimeout);
        config.put("readTimeout", readTimeout);

        log.debug("加载默认配置完成：{} [{}]", name, protocol);
    }

    /**
     * 验证处理器配置
     *
     * @throws IllegalArgumentException 如果配置无效
     */
    protected void validateConfig() {
        // 验证基本配置
        if (maxRetryCount < 0) {
            throw new IllegalArgumentException("maxRetryCount不能小于0");
        }
        if (retryInterval < 0) {
            throw new IllegalArgumentException("retryInterval不能小于0");
        }
        if (connectTimeout < 0) {
            throw new IllegalArgumentException("connectTimeout不能小于0");
        }
        if (readTimeout < 0) {
            throw new IllegalArgumentException("readTimeout不能小于0");
        }

        log.debug("处理器配置验证通过：{} [{}]", name, protocol);
    }

    /**
     * 验证上报数据
     *
     * @param data 上报数据
     * @throws IllegalArgumentException 如果数据无效
     */
    protected void validateData(ReportData data) {
        if (data == null) {
            throw new IllegalArgumentException("上报数据不能为null");
        }
        if (data.getPointCode() == null || data.getPointCode().isEmpty()) {
            throw new IllegalArgumentException("点位编码不能为空");
        }
        if (data.getProperties() == null) {
            throw new IllegalArgumentException("数据值不能为null");
        }

        // 可以根据需要添加更多验证规则
    }

    /**
     * 验证上报配置
     *
     * @param config 上报配置
     * @throws IllegalArgumentException 如果配置无效
     */
    protected void validateConfig(ReportConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("上报配置不能为null");
        }
        if (!config.validate()) {
            throw new IllegalArgumentException("上报配置无效：" + config.getTargetId());
        }
        if (!protocol.equals(config.getProtocol())) {
            throw new IllegalArgumentException("协议类型不匹配：期望" + protocol + "，实际" + config.getProtocol());
        }

        // 可以根据需要添加更多验证规则
    }

    /**
     * 数据预处理
     *
     * @param data 原始数据
     * @param config 上报配置
     * @return 处理后的数据
     */
    protected ReportData preProcessData(ReportData data, ReportConfig config) {
        // 默认实现，直接返回原始数据
        // 子类可以重写此方法进行数据转换、加密等操作
        return data;
    }

    /**
     * 上报后处理
     *
     * @param result 上报结果
     * @param data 原始数据
     * @param config 上报配置
     */
    protected void postProcessReport(ReportResult result, ReportData data, ReportConfig config) {
        // 默认实现，不做任何处理
        // 子类可以重写此方法进行结果分析、日志记录等操作
    }

    /**
     * 更新处理器配置
     *
     * @param config 上报配置
     */
    protected void updateHandlerConfig(ReportConfig config) {
        // 更新基础配置
        if (config.getMaxRetryCount() != null) {
            this.maxRetryCount = config.getMaxRetryCount();
        }
        if (config.getRetryInterval() != null) {
            this.retryInterval = config.getRetryInterval();
        }
        if (config.getConnectTimeout() != null) {
            this.connectTimeout = config.getConnectTimeout();
        }
        if (config.getReadTimeout() != null) {
            this.readTimeout = config.getReadTimeout();
        }

        log.debug("处理器配置已更新：{} [{}]", name, protocol);
    }

    /**
     * 记录单次上报统计
     *
     * @param success 是否成功
     * @param costTime 耗时（毫秒）
     * @param dataSize 数据大小
     */
    protected void recordReport(boolean success, long costTime, long dataSize) {
        totalReportCount.incrementAndGet();
        totalCostTime.addAndGet(costTime);
        totalDataSize.addAndGet(dataSize);

        if (success) {
            successReportCount.incrementAndGet();
        } else {
            failureReportCount.incrementAndGet();
        }
    }

    /**
     * 记录批量上报统计
     *
     * @param successCount 成功数量
     * @param failureCount 失败数量
     * @param totalCount 总数量
     * @param totalSize 总数据大小
     * @param totalTime 总耗时
     */
    protected void recordBatchReport(int successCount, int failureCount,
                                     int totalCount, long totalSize, long totalTime) {
        totalReportCount.addAndGet(totalCount);
        successReportCount.addAndGet(successCount);
        failureReportCount.addAndGet(failureCount);
        totalCostTime.addAndGet(totalTime);
        totalDataSize.addAndGet(totalSize);
    }

    /**
     * 从配置中获取值
     *
     * @param key 配置键
     * @param defaultValue 默认值
     * @return 配置值
     */
    protected Object getConfigValue(String key, Object defaultValue) {
        return config.getOrDefault(key, defaultValue);
    }

    /**
     * 从配置中获取字符串值
     *
     * @param key 配置键
     * @param defaultValue 默认值
     * @return 字符串值
     */
    protected String getStringConfig(String key, String defaultValue) {
        Object value = getConfigValue(key, defaultValue);
        return value != null ? value.toString() : defaultValue;
    }

    /**
     * 从配置中获取整数值
     *
     * @param key 配置键
     * @param defaultValue 默认值
     * @return 整数值
     */
    protected int getIntConfig(String key, int defaultValue) {
        Object value = getConfigValue(key, defaultValue);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    /**
     * 从配置中获取长整数值
     *
     * @param key 配置键
     * @param defaultValue 默认值
     * @return 长整数值
     */
    protected long getLongConfig(String key, long defaultValue) {
        Object value = getConfigValue(key, defaultValue);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    /**
     * 从配置中获取布尔值
     *
     * @param key 配置键
     * @param defaultValue 默认值
     * @return 布尔值
     */
    protected boolean getBooleanConfig(String key, boolean defaultValue) {
        Object value = getConfigValue(key, defaultValue);
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        } else if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        return defaultValue;
    }

    /**
     * 从配置中获取浮点数值
     *
     * @param key 配置键
     * @param defaultValue 默认值
     * @return 浮点数值
     */
    protected double getDoubleConfig(String key, double defaultValue) {
        Object value = getConfigValue(key, defaultValue);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }
}