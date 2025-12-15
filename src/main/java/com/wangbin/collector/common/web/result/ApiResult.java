package com.wangbin.collector.common.web.result;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * 统一API响应结果
 */
@Data
public class ApiResult<T> {

    private int code;
    private String message;
    private T data;
    private long timestamp;
    private Map<String, Object> extra;

    public ApiResult() {
        this.timestamp = System.currentTimeMillis();
    }

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

    public static <T> ApiResult<T> success(T data) {
        ApiResult<T> result = new ApiResult<>();
        result.setCode(ResultCode.SUCCESS.getCode());
        result.setMessage(ResultCode.SUCCESS.getMessage());
        result.setData(data);
        return result;
    }

    public static <T> ApiResult<T> success(String message, T data) {
        ApiResult<T> result = new ApiResult<>();
        result.setCode(ResultCode.SUCCESS.getCode());
        result.setMessage(message);
        result.setData(data);
        return result;
    }

    /**
     * 失败响应
     */
    public static <T> ApiResult<T> error() {
        return error(ResultCode.SYSTEM_ERROR.getCode(), ResultCode.SYSTEM_ERROR.getMessage());
    }

    public static <T> ApiResult<T> error(String message) {
        return error(ResultCode.SYSTEM_ERROR.getCode(), message);
    }

    public static <T> ApiResult<T> error(int code, String message) {
        ApiResult<T> result = new ApiResult<>();
        result.setCode(code);
        result.setMessage(message);
        return result;
    }

    public static <T> ApiResult<T> error(ResultCode resultCode) {
        return error(resultCode.getCode(), resultCode.getMessage());
    }

    /**
     * 是否成功
     */
    public boolean isSuccess() {
        return this.code == ResultCode.SUCCESS.getCode();
    }

    /**
     * 添加额外信息
     */
    public void addExtra(String key, Object value) {
        if (this.extra == null) {
            this.extra = new HashMap<>();
        }
        this.extra.put(key, value);
    }

    /**
     * 获取额外信息
     */
    public Object getExtra(String key) {
        return this.extra != null ? this.extra.get(key) : null;
    }

    /**
     * 设置分页信息
     */
    public void setPageInfo(long total, int page, int size) {
        Map<String, Object> pageInfo = new HashMap<>();
        pageInfo.put("total", total);
        pageInfo.put("page", page);
        pageInfo.put("size", size);
        pageInfo.put("pages", (total + size - 1) / size);

        if (this.extra == null) {
            this.extra = new HashMap<>();
        }
        this.extra.put("pageInfo", pageInfo);
    }

    /**
     * 设置请求ID
     */
    public void setRequestId(String requestId) {
        if (this.extra == null) {
            this.extra = new HashMap<>();
        }
        this.extra.put("requestId", requestId);
    }

    /**
     * 设置处理时间
     */
    public void setProcessTime(long processTime) {
        if (this.extra == null) {
            this.extra = new HashMap<>();
        }
        this.extra.put("processTime", processTime);
    }
}
