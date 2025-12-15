package com.wangbin.collector.core.report.model.auth;

import lombok.Data;

/**
 * 认证信息
 */
@Data
public class AuthInfo {
    private String productKey;
    private String deviceName;
    private String clientId;
    private String username;
    private String password; // SHA-256加密后的密码
    private String protocol = "json"; // json 或 binary
    private String brokerUrl; // 服务器地址
}