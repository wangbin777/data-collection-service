package com.wangbin.collector.core.report.model;

import java.util.Objects;

/**
 * 描述一次上报所使用的网关/云端子设备身份映射。
 */
public record ReportIdentity(String gatewayDeviceId, String cloudDeviceId, String productKey) {

    public ReportIdentity {
        Objects.requireNonNull(gatewayDeviceId, "gatewayDeviceId");
        Objects.requireNonNull(cloudDeviceId, "cloudDeviceId");
        if (productKey == null) {
            productKey = "";
        }
    }

    public String cacheKey() {
        return gatewayDeviceId + "|" + cloudDeviceId;
    }
}
