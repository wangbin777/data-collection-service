package com.wangbin.collector.core.cache.aspect;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.cache.manager.MultiLevelCacheManager;
import com.wangbin.collector.core.cache.model.CacheKey;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.HashMap;
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
    @Pointcut("execution(* com.wangbin.collector.core.collector.protocol.base.BaseCollector.readPoint(..))")
    public void readPointPointcut() {
    }

    /**
     * 定义切点：拦截BaseCollector类及其子类的readPoints方法
     */
    @Pointcut("execution(* com.wangbin.collector.core.collector.protocol.base.BaseCollector.readPoints(..))")
    public void readPointsPointcut() {
    }

    /**
     * 拦截readPoint方法，在方法执行后将结果保存到缓存
     */
    @AfterReturning(pointcut = "readPointPointcut()", returning = "result")
    public void afterReadPoint(JoinPoint joinPoint, Object result) {
        try {
            // 获取目标对象
            BaseCollector collector = (BaseCollector) joinPoint.getTarget();
            
            // 获取方法参数
            DataPoint point = (DataPoint) joinPoint.getArgs()[0];
            
            // 异步保存到缓存
            asyncSaveToCache(collector.getDeviceInfo().getDeviceId(), point, result);
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
            // 获取目标对象
            BaseCollector collector = (BaseCollector) joinPoint.getTarget();
            
            // 获取方法参数
            List<DataPoint> points = (List<DataPoint>) joinPoint.getArgs()[0];
            
            // 异步批量保存到缓存
            asyncBatchSaveToCache(collector.getDeviceInfo().getDeviceId(), points, result);
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
            // 创建缓存键
            CacheKey cacheKey = CacheKey.dataKey(deviceId, point.getPointId());
            
            // 保存到缓存
            multiLevelCacheManager.put(cacheKey, value);
            
            log.debug("异步缓存数据成功：{}.{} = {}", deviceId, point.getPointName(), value);
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
            // 构建批量缓存数据
            Map<CacheKey, T> cacheDataMap = new HashMap<>();
            for (DataPoint point : points) {
                T value = values.get(point.getPointId());
                if (value != null) {
                    // 创建缓存键
                    CacheKey cacheKey = CacheKey.dataKey(deviceId, point.getPointId());
                    cacheDataMap.put(cacheKey, value);
                }
            }
            
            // 批量保存到缓存
            if (!cacheDataMap.isEmpty()) {
                multiLevelCacheManager.putAll(cacheDataMap);
            }
            
            log.debug("异步批量缓存数据成功：{}，共 {} 个点位", deviceId, cacheDataMap.size());
        } catch (Exception e) {
            log.error("异步批量缓存数据失败", e);
        }
    }
}