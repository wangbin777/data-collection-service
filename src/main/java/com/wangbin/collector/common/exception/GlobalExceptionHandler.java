package com.wangbin.collector.common.exception;

import com.wangbin.collector.common.web.result.ApiResult;
import com.wangbin.collector.common.web.result.ResultCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.BindException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 全局异常处理器
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * 处理业务异常
     */
    @ExceptionHandler(BusinessException.class)
    public ApiResult<?> handleBusinessException(BusinessException e, HttpServletRequest request) {
        log.error("业务异常: {} - {}", e.getCode(), e.getMessage(), e);
        //ApiResult.error(e.getCode(), e.getMessage(), e.getData());
        return ApiResult.error(e.getCode(), e.getMessage());
    }

    /**
     * 处理采集器异常
     */
    @ExceptionHandler(CollectorException.class)
    public ApiResult<?> handleCollectorException(CollectorException e, HttpServletRequest request) {
        log.error("采集器异常 - Device: {}, Point: {}, Quality: {}",
                e.getDeviceId(), e.getPointId(), e.getDataQuality(), e);

        ApiResult<Object> result = ApiResult.error(e.getCode(), e.getMessage());
        result.setData(e.getData());

        // 添加额外信息
        result.addExtra("deviceId", e.getDeviceId());
        result.addExtra("pointId", e.getPointId());
        result.addExtra("dataQuality", e.getDataQuality().getCode());

        return result;
    }

    /**
     * 处理参数校验异常
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ApiResult<?> handleMethodArgumentNotValidException(MethodArgumentNotValidException e,
                                                              HttpServletRequest request) {
        List<FieldError> fieldErrors = e.getBindingResult().getFieldErrors();
        String message = fieldErrors.stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining("; "));

        log.error("参数校验异常: {}", message);
        return ApiResult.error(ResultCode.PARAM_ERROR.getCode(), message);
    }

    /**
     * 处理绑定异常
     */
    @ExceptionHandler(BindException.class)
    public ApiResult<?> handleBindException(BindException e, HttpServletRequest request) {
        List<FieldError> fieldErrors = e.getBindingResult().getFieldErrors();
        String message = fieldErrors.stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .collect(Collectors.joining("; "));

        log.error("参数绑定异常: {}", message);
        return ApiResult.error(ResultCode.PARAM_ERROR.getCode(), message);
    }

    /**
     * 处理约束违反异常
     */
    @ExceptionHandler(ConstraintViolationException.class)
    public ApiResult<?> handleConstraintViolationException(ConstraintViolationException e,
                                                           HttpServletRequest request) {
        String message = e.getConstraintViolations().stream()
                .map(violation -> violation.getPropertyPath() + ": " + violation.getMessage())
                .collect(Collectors.joining("; "));

        log.error("约束违反异常: {}", message);
        return ApiResult.error(ResultCode.PARAM_ERROR.getCode(), message);
    }

    /**
     * 处理其他异常
     */
    @ExceptionHandler(Exception.class)
    public ApiResult<?> handleException(Exception e, HttpServletRequest request) {
        String requestURI = request.getRequestURI();
        String method = request.getMethod();

        log.error("请求地址: {}, 请求方法: {}, 异常信息: {}", requestURI, method, e.getMessage(), e);

        // 生产环境隐藏详细错误信息
        String message = "系统内部错误，请联系管理员";
        if (isDevEnvironment()) {
            message = e.getMessage();
        }

        return ApiResult.error(ResultCode.SYSTEM_ERROR.getCode(), message);
    }

    /**
     * 判断是否为开发环境
     */
    private boolean isDevEnvironment() {
        String activeProfile = System.getProperty("spring.profiles.active");
        return "dev".equals(activeProfile) || "test".equals(activeProfile);
    }
}
