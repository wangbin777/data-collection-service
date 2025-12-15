package com.wangbin.collector.common.domain.dto.auth;

import lombok.Data;

/**
 * 认证响应DTO
 */
@Data
public class AuthResponse {

    private String version;
    private String method;
    private Integer code;
    private String message;
    private Long timestamp;
    private AuthData data;

    @Data
    public static class AuthData {
        private String token;
        private Long expireTime;
        private String clientId;
        private String deviceStatus;
        private String firmwareVersion;
    }

    public static AuthResponse success(String version, String method) {
        AuthResponse response = new AuthResponse();
        response.setVersion(version);
        response.setMethod(method);
        response.setCode(200);
        response.setMessage("认证成功");
        response.setTimestamp(System.currentTimeMillis());
        return response;
    }

    public static AuthResponse error(String version, String method, String message) {
        AuthResponse response = new AuthResponse();
        response.setVersion(version);
        response.setMethod(method);
        response.setCode(400);
        response.setMessage(message);
        response.setTimestamp(System.currentTimeMillis());
        return response;
    }
}
