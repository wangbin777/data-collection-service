package com.wangbin.collector.core.collector.scheduler;

import com.wangbin.collector.common.domain.entity.DataPoint;
import lombok.Data;
import java.util.List;

/**
 * 批次任务 - 表示一批要采集的数据点
 */
@Data
public class BatchTask {
    private String deviceId;
    private List<DataPoint> points;
    private int priority;  // 优先级
    private int retryCount = 0;  // 重试次数
    private long createTime = System.currentTimeMillis();

    public BatchTask(String deviceId, List<DataPoint> points, int priority) {
        this.deviceId = deviceId;
        this.points = points;
        this.priority = priority;
    }

    public boolean shouldRetry() {
        return retryCount < 3;  // 最多重试3次
    }

    public void incrementRetry() {
        retryCount++;
    }
}