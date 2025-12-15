package com.wangbin.collector.common.domain.dto.auth;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;


/**
 * 认证请求DTO
 */
@Data
public class AuthRequest {

    @NotBlank(message = "版本号不能为空")
    private String version;

    @NotBlank(message = "方法不能为空")
    private String method;

    private AuthParams params;

    @Data
    public static class AuthParams {
        @NotBlank(message = "产品Key不能为空")
        private String productKey;

        @NotBlank(message = "设备名称不能为空")
        private String deviceName;

        private String clientId;
        private String username;

        @NotBlank(message = "密码不能为空")
        private String password;

        private Long timestamp;
        private String sign;
    }
}
