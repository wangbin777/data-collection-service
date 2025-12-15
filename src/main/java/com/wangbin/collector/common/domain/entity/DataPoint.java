package com.wangbin.collector.common.domain.entity;

import lombok.Data;

import java.util.Date;
import java.util.Map;

/**
 * 数据点实体
 * 描述：数据采集系统中的数据点配置信息，对应设备或传感器上的一个可采集变量
 */
@Data
public class DataPoint {

    /**
     * 数据库主键ID（自增）
     */
    private Long id;

    /**
     * 数据点全局唯一标识符（系统内部使用）
     */
    private String pointId;

    /**
     * 数据点编码（业务唯一标识，用于外部系统识别）
     */
    private String pointCode;

    /**
     * 数据点名称（中文描述）
     */
    private String pointName;

    /**
     * 数据点别名（可选，用于显示）
     */
    private String pointAlias;

    /**
     * 所属设备ID
     */
    private String deviceId;

    /**
     * 所属设备名称
     */
    private String deviceName;

    /**
     * 所属设备分组ID
     */
    private String groupId;

    /**
     * 地址信息：根据不同协议有不同的格式
     * - Modbus: "40001" (保持寄存器)
     * - OPC: "Channel1.Device1.Tag1"
     * - SNMP: "1.3.6.1.4.1.2021.10.1.3.1"
     * - MQTT: "sensor/temperature"
     */
    private String address;

    /**
     * 数据类型
     * 可选值: INT, FLOAT, DOUBLE, BOOLEAN, STRING, BYTE, SHORT, LONG, UINT16, UINT32
     */
    private String dataType;

    /**
     * 读写权限
     * R: 只读（默认）
     * W: 只写
     * RW: 可读可写
     */
    private String readWrite;

    /**
     * 缩放因子（用于原始值转换）
     * 实际值 = 原始值 × scalingFactor + offset
     * 例如：原始值0-1000，实际温度0-100℃，缩放因子0.1
     */
    private Double scalingFactor;

    /**
     * 偏移量（用于原始值转换）
     * 实际值 = 原始值 × scalingFactor + offset
     * 例如：传感器有固定偏移-50℃
     */
    private Double offset;

    /**
     * 死区范围（避免频繁上报微小变化）
     * 例如：温度变化小于0.5℃时不更新
     */
    private Double deadband;

    /**
     * 单位
     * 例如：℃, %, Pa, m³/h, kW, rpm
     */
    private String unit;

    /**
     * 最小值（数据有效性校验）
     */
    private Double minValue;

    /**
     * 最大值（数据有效性校验）
     */
    private Double maxValue;

    /**
     * 采集模式
     * POLLING: 轮询采集（固定频率）
     * SUBSCRIPTION: 订阅采集（数据变化时上报）
     * EVENT: 事件触发采集
     */
    private String collectionMode;

    /**
     * 采集优先级（1-10，1为最高优先级）
     * 高优先级数据点会优先采集
     */
    private Integer priority;

    /**
     * 是否启用缓存
     * 0: 不启用
     * 1: 启用
     */
    private Integer cacheEnabled;

    /**
     * 缓存持续时间（秒）
     * 数据在缓存中保留的时间
     */
    private Integer cacheDuration;

    /**
     * 是否启用告警
     * 0: 不启用
     * 1: 启用
     */
    private Integer alarmEnabled;

    /**
     * 告警规则配置（JSON格式）
     * 示例: {"threshold": 100, "comparison": ">", "duration": 60}
     */
    private String alarmRule;

    /**
     * 数据点状态
     * 0: 禁用
     * 1: 启用（默认）
     * 2: 维护中
     * 3: 异常
     */
    private Integer status;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 更新时间
     */
    private Date updateTime;

    /**
     * 数据精度（小数位数）
     * 例如：2 表示保留2位小数
     */
    private Integer precision;

    /**
     * 备注信息
     */
    private String remark;

    /**
     * 附加配置信息（JSON格式）
     * 用于存储协议特定配置或其他扩展配置
     * 示例: {"byteOrder": "BIG_ENDIAN", "registerType": "HOLDING_REGISTER"}
     */
    private Map<String, Object> additionalConfig;

    /**
     * 获取实际值（考虑缩放因子和偏移量）
     * @param rawValue 原始采集值
     * @return 转换后的实际值
     */
    public Double getActualValue(Double rawValue) {
        if (rawValue == null) {
            return null;
        }
        double actual = rawValue;
        if (scalingFactor != null && scalingFactor != 0) {
            actual = actual * scalingFactor;
        }
        if (offset != null) {
            actual = actual + offset;
        }
        return actual;
    }

    /**
     * 判断值是否在有效范围内
     * @param value 待校验的值
     * @return true: 值有效; false: 值无效
     */
    public boolean isValueValid(Double value) {
        if (value == null) {
            return false;
        }
        if (minValue != null && value < minValue) {
            return false;
        }
        if (maxValue != null && value > maxValue) {
            return false;
        }
        return true;
    }

    /**
     * 判断数据点是否需要缓存
     * @return true: 需要缓存; false: 不需要缓存
     */
    public boolean needCache() {
        return cacheEnabled != null && cacheEnabled == 1;
    }

    /**
     * 判断数据点是否只读
     * @return true: 只读; false: 可写或可读可写
     */
    public boolean isReadOnly() {
        return "R".equals(readWrite);
    }

    /**
     * 判断数据点是否可写
     * @return true: 可写; false: 只读
     */
    public boolean isWritable() {
        return "W".equals(readWrite) || "RW".equals(readWrite);
    }

    /**
     * 获取死区范围（如果未设置则返回0）
     * @return 死区范围值
     */
    public double getDeadbandOrDefault() {
        return deadband != null ? deadband : 0.0;
    }

    /**
     * 获取缩放因子（如果未设置则返回1.0）
     * @return 缩放因子
     */
    public double getScalingFactorOrDefault() {
        return scalingFactor != null && scalingFactor != 0 ? scalingFactor : 1.0;
    }

    /**
     * 获取偏移量（如果未设置则返回0.0）
     * @return 偏移量
     */
    public double getOffsetOrDefault() {
        return offset != null ? offset : 0.0;
    }

    /**
     * 判断数据点是否启用
     * @return true: 启用; false: 禁用
     */
    public boolean isEnabled() {
        return status != null && status == 1;
    }

    /**
     * 获取附加配置值
     * @param key 配置键
     * @param defaultValue 默认值
     * @return 配置值
     */
    @SuppressWarnings("unchecked")
    public <T> T getAdditionalConfig(String key, T defaultValue) {
        if (additionalConfig == null) {
            return defaultValue;
        }
        T value = (T) additionalConfig.get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * 获取附加配置值
     * @param key 配置键
     * @return 配置值
     */
    @SuppressWarnings("unchecked")
    public <T> T getAdditionalConfig(String key) {
        if (additionalConfig == null) {
            return null;
        }
        T value = (T) additionalConfig.get(key);
        return value;
    }

    /**
     * 获取数据点完整标识
     * @return deviceId.pointCode
     */
    public String getFullIdentifier() {
        return deviceId + "." + pointCode;
    }

    /**
     * 格式化为标准JSON字符串
     * @return JSON格式字符串
     */
    @Override
    public String toString() {
        return String.format(
                "DataPoint{id=%d, pointCode='%s', pointName='%s', deviceId='%s', address='%s', dataType='%s', unit='%s', status=%d}",
                id, pointCode, pointName, deviceId, address, dataType, unit, status
        );
    }
}