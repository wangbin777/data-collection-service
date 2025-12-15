package com.wangbin.collector.common.domain.enums;

/**
 * 数据质量枚举
 */
public enum DataQuality {

    GOOD(100, "良好", "数据质量良好，可以正常使用"),
    UNCERTAIN(50, "不确定", "数据质量不确定，可能存在问题"),
    BAD(0, "不良", "数据质量不良，不可使用"),
    CONFIG_ERROR(10, "配置错误", "采集配置错误"),
    DEVICE_ERROR(20, "设备错误", "设备通信错误"),
    TIMEOUT(30, "超时", "采集超时"),
    VALUE_INVALID(40, "值无效", "采集值无效或超出范围"),
    PROCESS_ERROR(60, "处理错误", "数据处理错误"),
    CACHE_ERROR(70, "缓存错误", "数据缓存错误");

    private final int code;
    private final String description;
    private final String detail;

    DataQuality(int code, String description, String detail) {
        this.code = code;
        this.description = description;
        this.detail = detail;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public String getDetail() {
        return detail;
    }

    // 根据code获取枚举
    public static DataQuality fromCode(int code) {
        for (DataQuality quality : values()) {
            if (quality.getCode() == code) {
                return quality;
            }
        }
        return BAD;
    }

    // 判断数据是否可用
    public boolean isUsable() {
        return this.code >= 50;
    }

    // 判断是否需要告警
    public boolean needAlert() {
        return this.code < 50;
    }

    // 获取质量等级
    public String getQualityLevel() {
        if (code >= 80) {
            return "A";
        } else if (code >= 60) {
            return "B";
        } else if (code >= 40) {
            return "C";
        } else {
            return "D";
        }
    }
}
