package com.wangbin.collector.common.domain.dto.command;

import lombok.Data;

/**
 * 命令响应DTO
 */
@Data
public class CommandResponse {

    private String version;
    private String method;
    private String messageId;
    private Integer code;
    private String message;
    private Long timestamp;
    private Object data;

    public static CommandResponse success(String version, String method, String messageId, Object data) {
        CommandResponse response = new CommandResponse();
        response.setVersion(version);
        response.setMethod(method);
        response.setMessageId(messageId);
        response.setCode(200);
        response.setMessage("成功");
        response.setTimestamp(System.currentTimeMillis());
        response.setData(data);
        return response;
    }

    public static CommandResponse error(String version, String method, String messageId, String errorMessage) {
        CommandResponse response = new CommandResponse();
        response.setVersion(version);
        response.setMethod(method);
        response.setMessageId(messageId);
        response.setCode(500);
        response.setMessage(errorMessage);
        response.setTimestamp(System.currentTimeMillis());
        return response;
    }
}
