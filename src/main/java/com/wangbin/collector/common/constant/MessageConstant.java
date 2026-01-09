package com.wangbin.collector.common.constant;

import java.util.List;
import java.util.stream.Collectors;

public class MessageConstant {

    // 消息类型
    public static final String MESSAGE_TYPE_STATE_UPDATE = "thing.state.update";//设备状态更新
    public static final String MESSAGE_TYPE_PROPERTY_POST = "thing.property.post";//属性上报
    public static final String MESSAGE_TYPE_PROPERTY_SET = "thing.property.set";//属性设置
    public static final String MESSAGE_TYPE_EVENT_POST = "thing.event.post";//事件上报
    public static final String MESSAGE_TYPE_SERVICE_INVOKE = "thing.service.invoke";//服务调用
    public static final String MESSAGE_TYPE_CONFIG_PUSH = "thing.config.push";//配置推送
    public static final String MESSAGE_TYPE_OTA_UPGRADE = "thing.ota.upgrade";//OTA升级
    public static final String MESSAGE_TYPE_OTA_PROGRESS = "thing.ota.progress";//OTA进度上报
    public static final String MESSAGE_TYPE_AUTH = "auth";//设备认证

    // 消息版本
    public static final String MESSAGE_VERSION_1_0 = "1.0";
    public static final String MESSAGE_VERSION_1_1 = "1.1";
    public static final String MESSAGE_VERSION_2_0 = "2.0";

    // 认证字段
    public static final String FIELD_PRODUCT_KEY = "productKey";
    public static final String FIELD_DEVICE_NAME = "deviceName";
    public static final String FIELD_CLIENT_ID = "clientId";
    public static final String FIELD_USERNAME = "username";
    public static final String FIELD_PASSWORD = "password";
    public static final String FIELD_PARAMS = "params";
    public static final String FIELD_MESSAGE_ID = "messageId";
    public static final String FIELD_VERSION = "version";
    public static final String FIELD_METHOD = "method";

    // 数据上报相关
    public static final int DEFAULT_BATCH_SIZE = 50;
    public static final long DEFAULT_FLUSH_INTERVAL = 1000L;
    public static final int MAX_QUEUE_SIZE = 10000;

    private static final List<String> ACK_METHODS = List.of(
            MESSAGE_TYPE_STATE_UPDATE,
            MESSAGE_TYPE_PROPERTY_POST,
            MESSAGE_TYPE_PROPERTY_SET,
            MESSAGE_TYPE_EVENT_POST,
            MESSAGE_TYPE_SERVICE_INVOKE,
            MESSAGE_TYPE_CONFIG_PUSH,
            MESSAGE_TYPE_OTA_UPGRADE,
            MESSAGE_TYPE_OTA_PROGRESS
    );

    /**
     * 获取需要监听 ACK 的 method 列表
     */
    public static List<String> getAckMethods() {
        return ACK_METHODS;
    }

    /**
     * 将 method（以 . 分隔）转换成 MQTT topic 路径
     */
    public static String methodToTopicPath(String method) {
        if (method == null || method.isEmpty()) {
            return "";
        }
        return method.chars()
                .mapToObj(ch -> ch == '.' ? "/" : String.valueOf((char) ch))
                .collect(Collectors.joining());
    }
}
