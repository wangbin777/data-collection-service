package com.wangbin.collector.common.web.result;

/**
 * 响应码枚举
 */
public enum ResultCode {

    // 成功
    SUCCESS(200, "成功"),

    // 客户端错误
    BAD_REQUEST(400, "请求参数错误"),
    UNAUTHORIZED(401, "未授权"),
    FORBIDDEN(403, "禁止访问"),
    NOT_FOUND(404, "资源不存在"),
    METHOD_NOT_ALLOWED(405, "请求方法不允许"),

    // 业务错误
    PARAM_ERROR(1000, "参数错误"),
    DATA_NOT_FOUND(1001, "数据不存在"),
    DATA_EXISTS(1002, "数据已存在"),
    DATA_INVALID(1003, "数据无效"),
    OPERATION_FAILED(1004, "操作失败"),
    VALIDATION_FAILED(1005, "验证失败"),

    // 采集相关错误
    COLLECTOR_ERROR(2000, "采集器错误"),
    CONNECTION_ERROR(2001, "连接错误"),
    PROTOCOL_ERROR(2002, "协议错误"),
    DEVICE_ERROR(2003, "设备错误"),
    COLLECTION_ERROR(2004, "采集错误"),
    DATA_PROCESS_ERROR(2005, "数据处理错误"),
    REPORT_ERROR(2006, "数据上报错误"),

    // 配置相关错误
    CONFIG_ERROR(3000, "配置错误"),
    CONFIG_NOT_FOUND(3001, "配置不存在"),
    CONFIG_INVALID(3002, "配置无效"),
    CONFIG_LOAD_ERROR(3003, "配置加载错误"),

    // 认证授权错误
    AUTH_ERROR(4000, "认证错误"),
    AUTH_FAILED(4001, "认证失败"),
    TOKEN_INVALID(4002, "令牌无效"),
    TOKEN_EXPIRED(4003, "令牌过期"),
    PERMISSION_DENIED(4004, "权限不足"),

    // 系统错误
    SYSTEM_ERROR(5000, "系统内部错误"),
    SERVICE_UNAVAILABLE(5001, "服务不可用"),
    DATABASE_ERROR(5002, "数据库错误"),
    CACHE_ERROR(5003, "缓存错误"),
    NETWORK_ERROR(5004, "网络错误"),
    TIMEOUT_ERROR(5005, "超时错误"),

    // 第三方服务错误
    THIRD_PARTY_ERROR(6000, "第三方服务错误"),
    EXTERNAL_API_ERROR(6001, "外部API错误"),

    // 其他错误
    UNKNOWN_ERROR(9999, "未知错误");

    private final int code;
    private final String message;

    ResultCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    /**
     * 根据code获取枚举
     */
    public static ResultCode fromCode(int code) {
        for (ResultCode resultCode : values()) {
            if (resultCode.getCode() == code) {
                return resultCode;
            }
        }
        return UNKNOWN_ERROR;
    }

    /**
     * 判断是否为客户端错误
     */
    public boolean isClientError() {
        return code >= 400 && code < 500;
    }

    /**
     * 判断是否为服务器错误
     */
    public boolean isServerError() {
        return code >= 500;
    }

    /**
     * 判断是否为业务错误
     */
    public boolean isBusinessError() {
        return code >= 1000 && code < 2000;
    }
}
