package com.wangbin.collector.core.collector.scheduler;

import com.wangbin.collector.common.domain.entity.DataPoint;
import lombok.extern.slf4j.Slf4j;

/**
 * 自适应采集工具类
 * 实现数据变化率计算和采集频率自适应调整
 */
@Slf4j
public class AdaptiveCollectionUtil {
    
    /**
     * 默认最小采集间隔（毫秒）
     */
    public static final long DEFAULT_MIN_COLLECTION_INTERVAL = 100;
    
    /**
     * 默认最大采集间隔（毫秒）
     */
    public static final long DEFAULT_MAX_COLLECTION_INTERVAL = 3600000; // 1小时
    
    /**
     * 默认变化率阈值（百分比）
     */
    public static final double DEFAULT_CHANGE_THRESHOLD = 1.0;
    
    /**
     * 默认连续稳定次数阈值
     */
    public static final int DEFAULT_STABLE_THRESHOLD = 5;
    
    /**
     * 默认调整窗口（毫秒）
     */
    public static final long DEFAULT_ADJUST_WINDOW = 60000; // 1分钟
    
    /**
     * 计算数据变化率
     * 
     * @param currentValue 当前采集值
     * @param lastValue    上次采集值
     * @return 变化率（百分比），值为0表示无变化，值为100表示变化100%
     */
    public static double calculateChangeRate(Object currentValue, Object lastValue) {
        if (currentValue == null || lastValue == null) {
            return 0.0;
        }
        
        // 相同值直接返回0
        if (currentValue.equals(lastValue)) {
            return 0.0;
        }
        
        try {
            // 尝试转换为数值类型计算变化率
            double curr = Double.parseDouble(currentValue.toString());
            double last = Double.parseDouble(lastValue.toString());
            
            // 处理last为0的情况
            if (Math.abs(last) < 1e-6) {
                // 如果last为0，使用绝对差值作为变化率
                return Math.abs(curr - last);
            }
            
            // 计算相对变化率
            return Math.abs((curr - last) / last) * 100.0;
        } catch (NumberFormatException e) {
            // 非数值类型，变化即为100%
            return 100.0;
        }
    }
    
    /**
     * 根据数据变化率调整采集间隔
     * 
     * @param dataPoint      数据点对象
     * @param currentValue   当前采集值
     * @param changeRate     计算得到的变化率
     * @return 调整后的采集间隔（毫秒）
     */
    public static long adjustCollectionInterval(DataPoint dataPoint, Object currentValue, double changeRate) {
        if (dataPoint == null) {
            throw new IllegalArgumentException("数据点不能为空");
        }
        
        // 获取当前采集间隔
        long currentInterval = dataPoint.getCurrentCollectionInterval();
        
        // 获取基础配置
        long baseInterval = dataPoint.getBaseCollectionInterval();
        long minInterval = dataPoint.getMinCollectionInterval();
        long maxInterval = dataPoint.getMaxCollectionInterval();
        double changeThreshold = dataPoint.getChangeThreshold();
        
        // 获取数据点稳定计数
        int stableCount = dataPoint.getStableCount();
        
        long newInterval = currentInterval;
        
        // 比较变化率与阈值
        if (changeRate < changeThreshold) {
            // 数据稳定，增加稳定计数
            stableCount++;
            
            // 连续稳定次数达到阈值，延长采集间隔
            if (stableCount >= DEFAULT_STABLE_THRESHOLD) {
                // 指数增长，每次延长50%
                newInterval = (long) Math.min(currentInterval * 1.5, maxInterval);
                stableCount = 0; // 重置稳定计数
                log.debug("数据点 {} 连续稳定 {} 次，采集间隔从 {}ms 调整为 {}ms",
                        dataPoint.getPointId(), DEFAULT_STABLE_THRESHOLD, currentInterval, newInterval);
            }
        } else {
            // 数据波动，缩短采集间隔
            newInterval = (long) Math.max(currentInterval * 0.8, minInterval);
            stableCount = 0; // 重置稳定计数
            
            if (newInterval < currentInterval) {
                log.debug("数据点 {} 变化率 {}% 超过阈值 {}%，采集间隔从 {}ms 调整为 {}ms",
                        dataPoint.getPointId(), String.format("%.2f", changeRate), changeThreshold, currentInterval, newInterval);
            }
        }
        
        // 回归机制：防止长期偏离基础间隔
        if (Math.abs(newInterval - baseInterval) > baseInterval * 2) {
            newInterval = (long) (baseInterval + (newInterval - baseInterval) * 0.5);
            log.debug("数据点 {} 采集间隔长期偏离基础值，调整为回归值 {}ms",
                    dataPoint.getPointId(), newInterval);
        }
        
        // 更新数据点状态
        dataPoint.setLastValue(currentValue);
        dataPoint.setChangeRate(changeRate);
        dataPoint.setStableCount(stableCount);
        dataPoint.setCurrentCollectionInterval(newInterval);
        
        return newInterval;
    }
    
    /**
     * 初始化数据点的自适应采集配置
     * 
     * @param dataPoint         数据点对象
     * @param baseInterval      基础采集间隔（毫秒）
     * @param customMinInterval 自定义最小采集间隔（可选）
     * @param customMaxInterval 自定义最大采集间隔（可选）
     * @param customThreshold   自定义变化率阈值（可选）
     */
    public static void initDataPointAdaptiveConfig(DataPoint dataPoint, long baseInterval,
                                                  Long customMinInterval, Long customMaxInterval, Double customThreshold) {
        if (dataPoint == null) {
            throw new IllegalArgumentException("数据点不能为空");
        }
        
        // 设置基础采集间隔
        dataPoint.setBaseCollectionInterval(baseInterval);
        
        // 设置当前采集间隔（初始等于基础间隔）
        dataPoint.setCurrentCollectionInterval(baseInterval);
        
        // 设置最小采集间隔
        dataPoint.setMinCollectionInterval(customMinInterval != null ? customMinInterval : DEFAULT_MIN_COLLECTION_INTERVAL);
        
        // 设置最大采集间隔
        dataPoint.setMaxCollectionInterval(customMaxInterval != null ? customMaxInterval : DEFAULT_MAX_COLLECTION_INTERVAL);
        
        // 设置变化率阈值
        dataPoint.setChangeThreshold(customThreshold != null ? customThreshold : DEFAULT_CHANGE_THRESHOLD);
        
        // 初始化稳定计数
        dataPoint.setStableCount(0);
        
        // 初始化变化率
        dataPoint.setChangeRate(0.0);
        
        log.debug("数据点 {} 自适应采集配置初始化完成，基础间隔 {}ms，最小 {}ms，最大 {}ms，变化阈值 {}%",
                dataPoint.getPointId(), baseInterval, dataPoint.getMinCollectionInterval(),
                dataPoint.getMaxCollectionInterval(), dataPoint.getChangeThreshold());
    }
    
    /**
     * 检查是否需要调整采集间隔
     * 
     * @param lastAdjustTime 上次调整时间
     * @param adjustWindow   调整窗口（毫秒）
     * @return true：需要调整，false：不需要调整
     */
    public static boolean needAdjust(long lastAdjustTime, long adjustWindow) {
        return System.currentTimeMillis() - lastAdjustTime > adjustWindow;
    }
    
    /**
     * 重置数据点的自适应配置
     * 
     * @param dataPoint 数据点对象
     */
    public static void resetAdaptiveConfig(DataPoint dataPoint) {
        if (dataPoint == null) {
            return;
        }
        
        long baseInterval = dataPoint.getBaseCollectionInterval();
        dataPoint.setCurrentCollectionInterval(baseInterval);
        dataPoint.setStableCount(0);
        dataPoint.setChangeRate(0.0);
        dataPoint.setLastValue(null);
        
        log.debug("数据点 {} 自适应配置已重置，采集间隔恢复为基础值 {}ms",
                dataPoint.getPointId(), baseInterval);
    }
}