package com.wangbin.collector.common.domain.enums;
/**
 * 消息类型枚举
 */
public enum MessageType {

    // 设备状态
    STATE_UPDATE("thing.state.update", "设备状态更新"),

    // 设备属性
    PROPERTY_POST("thing.property.post", "属性上报"),
    PROPERTY_SET("thing.property.set", "属性设置"),

    // 设备事件
    EVENT_POST("thing.event.post", "事件上报"),

    // 设备服务
    SERVICE_INVOKE("thing.service.invoke", "服务调用"),

    // 设备配置
    CONFIG_PUSH("thing.config.push", "配置推送"),

    // OTA升级
    OTA_UPGRADE("thing.ota.upgrade", "OTA升级"),
    OTA_PROGRESS("thing.ota.progress", "OTA进度上报"),

    // 认证
    AUTH("auth", "设备认证"),

    // 系统消息
    HEARTBEAT("heartbeat", "心跳"),
    PING("ping", "PING"),
    PONG("pong", "PONG"),

    // 错误
    ERROR("error", "错误消息");

    private final String code;
    private final String description;

    MessageType(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    // 根据code获取枚举
    public static MessageType fromCode(String code) {
        for (MessageType type : values()) {
            if (type.getCode().equals(code)) {
                return type;
            }
        }
        return null;
    }

    // 判断是否为上行消息
    public boolean isUpstream() {
        return this == STATE_UPDATE || this == PROPERTY_POST ||
                this == EVENT_POST || this == OTA_PROGRESS;
    }

    // 判断是否为下行消息
    public boolean isDownstream() {
        return this == PROPERTY_SET || this == SERVICE_INVOKE ||
                this == CONFIG_PUSH || this == OTA_UPGRADE;
    }

    // 判断是否需要认证
    public boolean needAuth() {
        return this != AUTH && this != PING && this != PONG;
    }
}
