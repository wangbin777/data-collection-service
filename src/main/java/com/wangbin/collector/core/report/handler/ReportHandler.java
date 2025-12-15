package com.wangbin.collector.core.report.handler;

import com.wangbin.collector.core.report.model.ReportConfig;
import com.wangbin.collector.core.report.model.ReportData;
import com.wangbin.collector.core.report.model.ReportResult;

import java.util.List;
import java.util.Map;

/**
 * 报告处理器接口
 *
 * 职责：
 * 1. 定义数据上报的统一接口
 * 2. 支持不同协议的上报实现
 * 3. 处理上报的异常和重试
 * 4. 提供上报统计和监控
 */
public interface ReportHandler {

    /**
     * 获取处理器名称
     *
     * @return 处理器名称
     */
    String getName();

    /**
     * 获取支持的协议类型
     *
     * @return 协议类型
     */
    String getProtocol();

    /**
     * 初始化处理器
     *
     * @throws Exception 初始化失败时抛出异常
     */
    void init() throws Exception;

    /**
     * 上报单个数据
     *
     * @param data 上报数据
     * @param config 上报配置
     * @return 上报结果
     * @throws Exception 上报失败时抛出异常
     */
    ReportResult report(ReportData data, ReportConfig config) throws Exception;

    /**
     * 批量上报数据
     *
     * @param dataList 数据列表
     * @param config 上报配置
     * @return 上报结果列表
     * @throws Exception 上报失败时抛出异常
     */
    List<ReportResult> batchReport(List<ReportData> dataList, ReportConfig config) throws Exception;

    /**
     * 处理配置更新
     *
     * @param config 新的配置
     * @throws Exception 处理失败时抛出异常
     */
    void onConfigUpdate(ReportConfig config) throws Exception;

    /**
     * 处理配置删除
     *
     * @param config 被删除的配置
     * @throws Exception 处理失败时抛出异常
     */
    void onConfigRemove(ReportConfig config) throws Exception;

    /**
     * 获取处理器状态
     *
     * @return 状态信息映射
     */
    Map<String, Object> getStatus();

    /**
     * 获取处理器统计信息
     *
     * @return 统计信息映射
     */
    Map<String, Object> getStatistics();

    /**
     * 重置统计信息
     */
    void resetStatistics();

    /**
     * 销毁处理器
     *
     * @throws Exception 销毁失败时抛出异常
     */
    void destroy() throws Exception;
}