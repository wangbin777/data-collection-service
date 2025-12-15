package com.wangbin.collector.common.domain.dto.auth;

import lombok.Data;

import java.util.Date;

/**
 * 设备认证信息
 */
@Data
public class DeviceAuthInfo {

    private String deviceId;
    private String productKey;
    private String deviceName;
    private String clientId;
    private String username;
    private String passwordHash;
    private String token;
    private Date tokenExpireTime;
    private Integer status;
    private Date lastAuthTime;
    private Date createTime;
    private Date updateTime;

    // 验证是否有效
    public boolean isValid() {
        return status != null && status == 1
                && tokenExpireTime != null
                && tokenExpireTime.after(new Date());
    }
}
