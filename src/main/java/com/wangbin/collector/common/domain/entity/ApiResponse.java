package com.wangbin.collector.common.domain.entity;

public class ApiResponse<T> {
    private Integer code;
    private String msg;
    private T data;

    // getters and setters
    public Integer getCode() { return code; }
    public void setCode(Integer code) { this.code = code; }

    public String getMsg() { return msg; }
    public void setMsg(String msg) { this.msg = msg; }

    public T getData() { return data; }
    public void setData(T data) { this.data = data; }

    public boolean isSuccess() {
        return code != null && code == 0;
    }
}
