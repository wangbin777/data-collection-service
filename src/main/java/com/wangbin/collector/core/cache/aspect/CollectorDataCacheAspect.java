package com.wangbin.collector.core.cache.aspect;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.cache.manager.MultiLevelCacheManager;
import com.wangbin.collector.core.cache.model.CacheKey;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 采集数据缓存切面
 * 拦截数据采集方法，将采集结果保存到缓存中
 */
@Slf4j
@Aspect
@Component
public class CollectorDataCacheAspect {

    @Autowired
    private MultiLevelCacheManager multiLevelCacheManager;

    /**
     * 定义切点：拦截BaseCollector类及其子类的readPoint方法
     */
    @Pointcut("execution(* com.wangbin.collector.core.collector.protocol.base.ProtocolCollector.readPoint(..))")
    public void readPointPointcut() {
    }

    /**
     * 定义切点：拦截BaseCollector类及其子类的readPoints方法
     */
    @Pointcut("execution(* com.wangbin.collector.core.collector.protocol.base.ProtocolCollector.readPoints(..))")
    public void readPointsPointcut() {
    }

    /**
     * 拦截readPoint方法，在方法执行后将结果保存到缓存
     */
    @AfterReturning(pointcut = "readPointPointcut()", returning = "result")
    public void afterReadPoint(JoinPoint joinPoint, Object result) {
        try {
            if (result == null) {
                return;
            }

            DataPoint point = (DataPoint) joinPoint.getArgs()[0];
            if (point == null) {
                return;
            }

            String deviceId = point.getDeviceId();
            if (deviceId == null || deviceId.isEmpty()) {
                return;
            }

            asyncSaveToCache(deviceId, point, result);
        } catch (Exception e) {
            log.error("准备异步缓存数据失败", e);
        }
    }

    /**
     * 拦截readPoints方法，在方法执行后将结果保存到缓存
     */
    @AfterReturning(pointcut = "readPointsPointcut()", returning = "result")
    public void afterReadPoints(JoinPoint joinPoint, Map<String, Object> result) {
        try {
            if (result == null || result.isEmpty()) {
                return;
            }

            List<DataPoint> points = (List<DataPoint>) joinPoint.getArgs()[0];
            if (points == null || points.isEmpty()) {
                return;
            }

            String deviceId = points.stream()
                    .filter(point -> point != null && point.getDeviceId() != null)
                    .map(DataPoint::getDeviceId)
                    .findFirst()
                    .orElse(null);
            if (deviceId == null) {
                return;
            }

            asyncBatchSaveToCache(deviceId, points, result);
        } catch (Exception e) {
            log.error("准备批量异步缓存数据失败", e);
        }
    }

    /**
     * 异步保存到缓存
     */
    @Async("cacheAsyncExecutor")
    protected <T> void asyncSaveToCache(String deviceId, DataPoint point, T value) {
        try {
            // 1. 选择性缓存：检查是否需要缓存
            if (!shouldCache(point) || value == null) {
                log.debug("跳过缓存：{}.{}，原因：缓存未启用或非关键数据", deviceId, point.getPointName());
                return;
            }

            // 2. 创建缓存键
            CacheKey cacheKey = CacheKey.dataKey(deviceId, point.getPointId());
            
            // 3. 设置缓存过期时间
            long expireTime = getCacheExpireTime(point);
            
            // 4. 保存到缓存
            multiLevelCacheManager.put(cacheKey, value, expireTime);
            
            log.debug("异步缓存数据成功：{}.{} = {}, 过期时间：{}ms", deviceId, point.getPointName(), value, expireTime);
        } catch (Exception e) {
            log.error("异步缓存数据失败", e);
        }
    }

    /**
     * 异步批量保存到缓存
     */
    @Async("cacheAsyncExecutor")
    protected <T> void asyncBatchSaveToCache(String deviceId, List<DataPoint> points, Map<String, T> values) {
        try {
            // 1. 构建批量缓存数据（只缓存需要的数据）
            for (DataPoint point : points) {
                String pointId = point.getPointId();
                T value = values.get(pointId);
                
                // 条件处理：只缓存非null值且需要缓存的数据
                if (value != null && shouldCache(point)) {
                    CacheKey cacheKey = CacheKey.dataKey(deviceId, pointId);
                    long expireTime = getCacheExpireTime(point);
                    multiLevelCacheManager.put(cacheKey, value, expireTime);
                }
            }
            
            log.debug("异步批量缓存数据成功：{}，点位数量：{}", deviceId, points.size());
        } catch (Exception e) {
            log.error("异步批量缓存数据失败", e);
        }
    }
    
    /**
     * 判断数据点是否需要缓存
     */
    private boolean shouldCache(DataPoint point) {
        // 1. 检查数据点是否启用缓存
        if (point == null || !point.needCache()) {
            return false;
        }
        
        // 2. 可以添加更多缓存策略判断
        // 例如：只缓存优先级大于等于5的数据点
        /*
        if (point.getPriority() != null && point.getPriority() < 5) {
            return false;
        }
        */
        
        return true;
    }
    
    /**
     * 获取缓存过期时间
     */
    private long getCacheExpireTime(DataPoint point) {
        // 1. 如果数据点设置了缓存持续时间，使用该值
        if (point.getCacheDuration() != null && point.getCacheDuration() > 0) {
            return point.getCacheDuration() * 1000L;
        }
        
        // 2. 根据数据点优先级设置不同的过期时间
        if (point.getPriority() != null) {
            if (point.getPriority() <= 3) {
                // 高优先级数据缓存时间长一些（2小时）
                return 7200_000L;
            } else if (point.getPriority() <= 7) {
                // 中优先级数据缓存时间中等（1小时）
                return 3600_000L;
            }
        }
        
        // 3. 默认缓存时间（30分钟）
        return 1800_000L;
    }
}
