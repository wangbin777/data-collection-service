package com.wangbin.collector.core.collector.protocol.iec.util;

import com.beanit.iec61850bean.Fc;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.iec.domain.Iec61850Address;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * IEC61850 地址解析工具。
 */
public final class Iec61850AddressParser {

    private Iec61850AddressParser() {}

    public static Iec61850Address parse(DataPoint point) {
        return parse(point, Fc.ST);
    }

    public static Iec61850Address parse(DataPoint point, Fc defaultFc) {
        if (point == null) {
            throw new IllegalArgumentException("数据点不能为空");
        }

        String rawAddress = trim(point.getAddress());
        String fcSegment = null;

        if (rawAddress != null) {
            int splitIdx = findSeparator(rawAddress);
            if (splitIdx > -1) {
                fcSegment = rawAddress.substring(splitIdx + 1);
                rawAddress = rawAddress.substring(0, splitIdx);
            }
        }

        if ((rawAddress == null || rawAddress.isEmpty())) {
            rawAddress = buildAddressFromConfig(point.getAdditionalConfig());
        }

        if (rawAddress == null || rawAddress.isEmpty()) {
            throw new IllegalArgumentException("IEC61850地址不能为空, point=" + point.getPointName());
        }

        if (fcSegment == null) {
            fcSegment = findFcFromConfig(point.getAdditionalConfig());
        }

        return parse(rawAddress, fcSegment, defaultFc != null ? defaultFc : Fc.ST);
    }

    public static Iec61850Address parse(String expression, String fcOverride, Fc defaultFc) {
        String rawAddress = trim(expression);
        String fcSegment = trim(fcOverride);

        if (rawAddress != null) {
            int splitIdx = findSeparator(rawAddress);
            if (splitIdx > -1) {
                if (fcSegment == null) {
                    fcSegment = rawAddress.substring(splitIdx + 1);
                }
                rawAddress = rawAddress.substring(0, splitIdx);
            }
        }

        if (rawAddress == null || rawAddress.isEmpty()) {
            throw new IllegalArgumentException("IEC61850地址不能为空");
        }

        Fc fc = parseFc(fcSegment, defaultFc);
        return new Iec61850Address(rawAddress, fc, expression);
    }

    private static int findSeparator(String raw) {
        int idx = raw.indexOf('@');
        if (idx >= 0) {
            return idx;
        }
        idx = raw.indexOf(':');
        if (idx >= 0) {
            return idx;
        }
        return -1;
    }

    private static String buildAddressFromConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return null;
        }
        String ld = trim(config.get("logicalDevice"));
        String ln = trim(config.get("logicalNode"));
        String dataObject = trim(config.get("dataObject"));
        String dataAttribute = trim(config.get("dataAttribute"));
        String subAttribute = trim(config.get("subAttribute"));

        if (ld == null || ln == null) {
            return null;
        }

        List<String> parts = new ArrayList<>();
        if (dataObject != null) {
            parts.add(dataObject);
        }
        if (dataAttribute != null) {
            parts.add(dataAttribute);
        }
        if (subAttribute != null) {
            parts.add(subAttribute);
        }
        String suffix = String.join(".", parts);
        if (suffix.isEmpty()) {
            return ld + "/" + ln;
        }
        return ld + "/" + ln + "." + suffix;
    }

    private static String findFcFromConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return null;
        }
        Object fc = config.get("fc");
        if (fc == null) {
            fc = config.get("functionalConstraint");
        }
        if (fc == null) {
            fc = config.get("FC");
        }
        return trim(fc);
    }

    private static Fc parseFc(String fcText, Fc defaultFc) {
        if (fcText == null || fcText.isEmpty()) {
            return defaultFc != null ? defaultFc : Fc.ST;
        }
        try {
            return Fc.fromString(fcText.trim().toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            throw new IllegalArgumentException("非法的Functional Constraint: " + fcText, e);
        }
    }

    private static String trim(Object value) {
        if (value == null) {
            return null;
        }
        String text = Objects.toString(value, "").trim();
        return text.isEmpty() ? null : text;
    }
}
