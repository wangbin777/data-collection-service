package com.wangbin.collector.common.domain.enums;

import lombok.Getter;

/**
 * 连接状态枚举
 */
@Getter
public enum ConnectionStatus {

    DISCONNECTED("DISCONNECTED", "已断开", 0),
    CONNECTING("CONNECTING", "连接中", 1),
    CONNECTED("CONNECTED", "已连接", 2),
    AUTHENTICATING("AUTHENTICATING", "认证中", 3),
    AUTHENTICATED("AUTHENTICATED", "已认证", 4),
    ERROR("ERROR", "错误", 5),
    RECONNECTING("RECONNECTING", "重连中", 6);

    private final String code;
    private final String description;
    private final int level;

    ConnectionStatus(String code, String description, int level) {
        this.code = code;
        this.description = description;
        this.level = level;
    }

    // 根据code获取枚举
    public static ConnectionStatus fromCode(String code) {
        for (ConnectionStatus status : values()) {
            if (status.getCode().equals(code)) {
                return status;
            }
        }
        return DISCONNECTED;
    }

    // 判断是否已连接
    public boolean isConnected() {
        return this == CONNECTED || this == AUTHENTICATED;
    }

    // 判断是否可以发送数据
    public boolean canSendData() {
        return this == AUTHENTICATED;
    }

    // 判断是否需要重连
    public boolean needReconnect() {
        return this == DISCONNECTED || this == ERROR;
    }

    // 判断是否正在进行连接操作
    public boolean isConnecting() {
        return this == CONNECTING || this == AUTHENTICATING || this == RECONNECTING;
    }
}
