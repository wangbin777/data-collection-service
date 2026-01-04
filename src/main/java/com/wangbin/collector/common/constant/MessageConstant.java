package com.wangbin.collector.common.constant;

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
}
