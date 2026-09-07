package com.wangbin.collector.common.web.result;


import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.wangbin.collector.common.constant.CommonMapKeys;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * 统一 API 响应结果。
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResult<T> {

    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_ERROR = "error";

    private Integer code;
    private String status;
    @JsonAlias("msg")
    private String message;
    private T data;
    private Long timestamp;
    private Map<String, Object> extra;
    private String deviceId;
    private Integer count;
    private Boolean running;

    /**
     * 创建响应结果并记录生成时间。
     */
    public ApiResult() {
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * 创建带业务码的响应结果。
     */
    public ApiResult(int code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * 成功响应
     */
    public static <T> ApiResult<T> success() {
        return success(null);
    }

    /**
     * 构造带 code 的成功响应，用于既有 ApiResult 接口。
     */
    public static <T> ApiResult<T> success(T data) {
        ApiResult<T> result = new ApiResult<>();
        result.setCode(ResultCode.SUCCESS.getCode());
        result.setMessage(ResultCode.SUCCESS.getMessage());
        result.setData(data);
        return result;
    }

    /**
     * 构造带 code 的成功响应，用于既有 ApiResult 接口。
     */
    public static <T> ApiResult<T> success(String message, T data) {
        ApiResult<T> result = new ApiResult<>();
        result.setCode(ResultCode.SUCCESS.getCode());
        result.setMessage(message);
        result.setData(data);
        return result;
    }

    /**
     * 失败响应。
     */
    public static <T> ApiResult<T> error() {
        return error(ResultCode.SYSTEM_ERROR.getCode(), ResultCode.SYSTEM_ERROR.getMessage());
    }

    /**
     * 构造带 code 的失败响应，用于既有 ApiResult 接口。
     */
    public static <T> ApiResult<T> error(String message) {
        return error(ResultCode.SYSTEM_ERROR.getCode(), message);
    }

    /**
     * 构造带 code 的失败响应，用于既有 ApiResult 接口。
     */
    public static <T> ApiResult<T> error(int code, String message) {
        ApiResult<T> result = new ApiResult<>();
        result.setCode(code);
        result.setMessage(message);
        return result;
    }

    /**
     * 构造带 code 的失败响应，用于既有 ApiResult 接口。
     */
    public static <T> ApiResult<T> error(ResultCode resultCode) {
        return error(resultCode.getCode(), resultCode.getMessage());
    }

    /**
     * 构造 status 风格成功响应，兼容管理接口历史 JSON。
     */
    public static <T> ApiResult<T> statusSuccess(String message, T data) {
        ApiResult<T> result = new ApiResult<>();
        result.setCode(ResultCode.SUCCESS.getCode());
        result.setStatus(STATUS_SUCCESS);
        result.setMessage(message);
        result.setData(data);
        return result;
    }

    /**
     * 构造 status 风格失败响应，兼容管理接口历史 JSON。
     */
    public static <T> ApiResult<T> statusError(String message, T data) {
        ApiResult<T> result = new ApiResult<>();
        result.setStatus(STATUS_ERROR);
        result.setMessage(message);
        result.setData(data);
        return result;
    }

    /**
     * 构建设备控制成功响应，保留 deviceId 顶层字段。
     */
    public static ApiResult<Object> deviceSuccess(String deviceId, String message) {
        ApiResult<Object> result = statusSuccess(message, null);
        result.setDeviceId(deviceId);
        return result;
    }

    /**
     * 构建设备控制失败响应，保留 deviceId 顶层字段。
     */
    public static ApiResult<Object> deviceError(String deviceId, String message) {
        ApiResult<Object> result = statusError(message, null);
        result.setDeviceId(deviceId);
        return result;
    }

    /**
     * 构建设备控制数据响应，保留 deviceId 顶层字段。
     */
    public static <T> ApiResult<T> deviceSuccessData(String deviceId, T data) {
        ApiResult<T> result = statusSuccess(null, data);
        result.setDeviceId(deviceId);
        return result;
    }

    /**
     * 设置设备控制响应的数量字段。
     */
    public ApiResult<T> withCount(Integer count) {
        this.count = count;
        return this;
    }

    /**
     * 设置设备控制响应的本地设备标识。
     */
    public ApiResult<T> withDeviceId(String deviceId) {
        this.deviceId = deviceId;
        return this;
    }

    /**
     * 设置设备运行态字段。
     */
    public ApiResult<T> withRunning(Boolean running) {
        this.running = running;
        return this;
    }

    /**
     * 是否成功。
     */
    public boolean isSuccess() {
        if (this.code != null) {
            return this.code == ResultCode.SUCCESS.getCode() || this.code == 0;
        }
        return STATUS_SUCCESS.equalsIgnoreCase(this.status);
    }

    /**
     * 添加额外信息。
     */
    public void addExtra(String key, Object value) {
        if (this.extra == null) {
            this.extra = new HashMap<>();
        }
        this.extra.put(key, value);
    }

    /**
     * 获取额外信息。
     */
    public Object getExtra(String key) {
        return this.extra != null ? this.extra.get(key) : null;
    }

    /**
     * 设置分页信息。
     */
    public void setPageInfo(long total, int page, int size) {
        Map<String, Object> pageInfo = new HashMap<>();
        pageInfo.put(CommonMapKeys.TOTAL, total);
        pageInfo.put(CommonMapKeys.PAGE, page);
        pageInfo.put(CommonMapKeys.SIZE, size);
        pageInfo.put(CommonMapKeys.PAGES, (total + size - 1) / size);

        if (this.extra == null) {
            this.extra = new HashMap<>();
        }
        this.extra.put(CommonMapKeys.PAGE_INFO, pageInfo);
    }

    /**
     * 设置请求 ID。
     */
    public void setRequestId(String requestId) {
        if (this.extra == null) {
            this.extra = new HashMap<>();
        }
        this.extra.put(CommonMapKeys.REQUEST_ID, requestId);
    }

    /**
     * 设置处理时间。
     */
    public void setProcessTime(long processTime) {
        if (this.extra == null) {
            this.extra = new HashMap<>();
        }
        this.extra.put(CommonMapKeys.PROCESS_TIME, processTime);
    }
}
