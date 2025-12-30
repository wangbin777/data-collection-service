package com.wangbin.collector.monitor.metrics;

import lombok.Builder;
import lombok.Data;

/**
 * 简要的异常汇总信息。
 */
@Data
@Builder
public class ExceptionSummary {
    private final String deviceId;
    private final String pointId;
    private final String category;
    private final String message;
    private final long timestamp;
}
