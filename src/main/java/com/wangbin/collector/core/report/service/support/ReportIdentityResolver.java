
package com.wangbin.collector.core.report.service.support;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.enums.QualityEnum;
import com.wangbin.collector.core.processor.ProcessResult;
import com.wangbin.collector.core.report.model.ReportIdentity;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Resolves report identities (productKey/deviceName) from point configuration.
 */
@Component
public class ReportIdentityResolver {

    public ProcessResult toProcessResult(Object cacheValue) {
        if (cacheValue instanceof ProcessResult processResult) {
            return processResult;
        }
        ProcessResult result = new ProcessResult();
        result.setSuccess(true);
        result.setRawValue(cacheValue);
        result.setProcessedValue(cacheValue);
        result.setQuality(QualityEnum.GOOD.getCode());
        return result;
    }

    public List<ReportIdentity> resolve(String gatewayDeviceId,
                                         DataPoint point,
                                         String defaultProductKey) {
        List<ReportIdentity> fromBindings = resolveBindings(gatewayDeviceId, point, defaultProductKey);
        if (!fromBindings.isEmpty()) {
            return fromBindings;
        }
        return resolveLegacyIdentities(gatewayDeviceId, point, defaultProductKey);
    }

    private List<ReportIdentity> resolveBindings(String gatewayDeviceId,
                                                 DataPoint point,
                                                 String defaultProductKey) {
        if (point == null) {
            return Collections.emptyList();
        }
        Object bindingsConfig = point.getAdditionalConfig("reportBindings");
        if (bindingsConfig == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> bindings = new ArrayList<>();
        if (bindingsConfig instanceof Collection<?> collection) {
            for (Object item : collection) {
                Map<String, Object> map = asMap(item);
                if (map != null) {
                    bindings.add(map);
                }
            }
        } else if (bindingsConfig.getClass().isArray()) {
            int len = java.lang.reflect.Array.getLength(bindingsConfig);
            for (int i = 0; i < len; i++) {
                Map<String, Object> map = asMap(java.lang.reflect.Array.get(bindingsConfig, i));
                if (map != null) {
                    bindings.add(map);
                }
            }
        } else {
            Map<String, Object> map = asMap(bindingsConfig);
            if (map != null) {
                bindings.add(map);
            }
        }
        List<ReportIdentity> result = new ArrayList<>();
        for (Map<String, Object> map : bindings) {
            Object deviceName = map.get("deviceName");
            Object pk = map.getOrDefault("productKey", map.get("reportProductKey"));
            String name = normalizeString(deviceName);
            if (name == null || name.isEmpty()) {
                continue;
            }
            String productKey = normalizeString(pk);
            if (productKey == null || productKey.isEmpty()) {
                productKey = defaultProductKey;
            }
            result.add(new ReportIdentity(gatewayDeviceId, name, productKey));
        }
        return result;
    }

    private Map<String, Object> asMap(Object raw) {
        if (raw instanceof Map<?, ?> map) {
            Map<String, Object> result = new HashMap<>();
            map.forEach((k, v) -> {
                if (k != null) {
                    result.put(String.valueOf(k), v);
                }
            });
            return result;
        }
        return null;
    }

    private List<ReportIdentity> resolveLegacyIdentities(String gatewayDeviceId,
                                                         DataPoint point,
                                                         String defaultProductKey) {
        LinkedHashSet<String> deviceNames = toOrderedSet(point != null ? point.getAdditionalConfig("reportDeviceName") : null);
        if (deviceNames.isEmpty()) {
            return Collections.emptyList();
        }
        LinkedHashSet<String> productKeys = toOrderedSet(point != null
                ? Optional.ofNullable(point.getAdditionalConfig("productKey"))
                .orElse(point.getAdditionalConfig("reportProductKey"))
                : null);
        List<ReportIdentity> result = new ArrayList<>(deviceNames.size());
        List<String> pkList = new ArrayList<>(productKeys);
        boolean sameSize = !pkList.isEmpty() && pkList.size() == deviceNames.size();
        String fallbackPk = pkList.isEmpty() ? defaultProductKey : pkList.get(0);
        int index = 0;
        for (String name : deviceNames) {
            if (name == null || name.isEmpty()) {
                continue;
            }
            String pk = sameSize ? pkList.get(index) : fallbackPk;
            if (pk == null || pk.isEmpty()) {
                pk = defaultProductKey;
            }
            result.add(new ReportIdentity(gatewayDeviceId, name, pk));
            index++;
        }
        return result;
    }

    private LinkedHashSet<String> toOrderedSet(Object configured) {
        LinkedHashSet<String> result = new LinkedHashSet<>();
        if (configured == null) {
            return result;
        }
        if (configured instanceof Collection<?> collection) {
            for (Object item : collection) {
                addString(result, item);
            }
        } else if (configured.getClass().isArray()) {
            int length = java.lang.reflect.Array.getLength(configured);
            for (int i = 0; i < length; i++) {
                addString(result, java.lang.reflect.Array.get(configured, i));
            }
        } else {
            addString(result, configured);
        }
        return result;
    }

    private void addString(Set<String> bucket, Object raw) {
        String normalized = normalizeString(raw);
        if (normalized == null || normalized.isEmpty()) {
            return;
        }
        if (normalized.contains(",")) {
            for (String part : normalized.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    bucket.add(trimmed);
                }
            }
            return;
        }
        bucket.add(normalized);
    }

    private String normalizeString(Object raw) {
        if (raw == null) {
            return null;
        }
        String text = String.valueOf(raw).trim();
        return text.isEmpty() ? null : text;
    }
}
