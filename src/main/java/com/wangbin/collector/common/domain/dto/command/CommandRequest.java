package com.wangbin.collector.common.domain.dto.command;

import lombok.Data;

import java.util.Map;

/**
 * 命令请求DTO
 */
@Data
public class CommandRequest {

    private String version;
    private String method;
    private String messageId;
    private Long timestamp;
    private Map<String, Object> params;
    private String sign;

    // 验证方法
    public boolean isValid() {
        return version != null && method != null && timestamp != null;
    }
}
