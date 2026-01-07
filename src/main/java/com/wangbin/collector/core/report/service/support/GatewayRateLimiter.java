
package com.wangbin.collector.core.report.service.support;

import com.google.common.util.concurrent.RateLimiter;
import com.wangbin.collector.core.report.config.ReportProperties;
import org.springframework.stereotype.Component;

/**
 * ???????????????????
 */
@Component
public class GatewayRateLimiter {

    private final RateLimiter rateLimiter;

    public GatewayRateLimiter(ReportProperties reportProperties) {
        int maxPerSecond = reportProperties.getMaxGatewayMessagesPerSecond();
        this.rateLimiter = maxPerSecond > 0 ? RateLimiter.create(maxPerSecond) : null;
    }

    public boolean tryAcquire(boolean highPriority) {
        if (highPriority || rateLimiter == null) {
            return true;
        }
        return rateLimiter.tryAcquire();
    }
}
