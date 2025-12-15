package com.wangbin.collector.common.domain.entity;

import lombok.Data;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 采集配置实体类
 * 用于定义和管理数据采集的各项配置参数
 */
@Data
public class CollectionConfig {

    // ==================== 基础信息 ====================

    /** 数据库主键ID */
    private Long id;

    /** 配置唯一标识符 */
    private String configId;

    /** 配置名称（便于识别和管理） */
    private String configName;

    /** 配置类型: DEVICE(设备级), GROUP(分组级), SYSTEM(系统级) */
    private String configType;

    /** 目标ID：设备ID或分组ID，表示此配置应用于哪个目标 */
    private String targetId;

    // ==================== 设备基础信息（冗余字段）====================

    /** 设备名称（展示必需） */
    private String deviceName;

    /** 设备IP地址（连接必需） */
    private String deviceIp;

    /** 设备端口（连接必需） */
    private Integer devicePort;

    /** 协议类型（一致性校验） */
    private String deviceProtocolType;

    /** 设备别名（用户友好） */
    private String deviceAlias;

    /** 采集间隔（高频使用），单位：秒 */
    private Integer collectionInterval;

    /** 设备位置（业务需要时） */
    private String deviceLocation;

    /** 产品型号（设备管理） */
    private String productModel;

    /** 所属部门（权限控制） */
    private String department;

    // ==================== 协议与连接配置 ====================

    /** 协议类型: MODBUS, OPC_UA, SNMP, MQTT等 */
    private String protocolType;

    /** 连接类型: TCP, UDP, HTTP, WEBSOCKET等 */
    private String connectionType;

    /** 协议参数配置，JSON格式存储 */
    private Map<String, Object> protocolParams;

    /** 连接参数配置，JSON格式存储 */
    private Map<String, Object> connectionParams;

    // ==================== 采集相关参数 ====================

    /** 采集参数配置：采集间隔、重试次数等 */
    private Map<String, Object> collectionParams;

    /** 数据上报参数配置：上报间隔、批量大小等 */
    private Map<String, Object> reportParams;

    /** 数据处理参数配置：数据转换、校验规则等 */
    private Map<String, Object> processParams;

    // ==================== 配置状态与版本 ====================

    /** 配置状态: 0-禁用, 1-启用, 2-测试中 */
    private Integer status;

    /** 配置生效时间 */
    private Date effectiveTime;

    /** 配置过期时间 */
    private Date expireTime;

    /** 配置版本号，用于配置更新和回滚 */
    private Integer version;

    // ==================== 操作记录 ====================

    /** 最后操作人员 */
    private String lastOperator;

    /** 记录创建时间 */
    private Date createTime;

    /** 记录最后更新时间 */
    private Date updateTime;

    /** 配置备注信息 */
    private String remark;

    // ==================== 配置项列表 ====================

    /** 详细的配置项列表，用于灵活的参数配置 */
    private List<ConfigItem> configItems;

    /**
     * 配置项内部类
     * 表示一个具体的配置参数项
     */
    @Data
    public static class ConfigItem {
        /** 配置项键（唯一标识） */
        private String key;

        /** 配置项名称（显示用） */
        private String name;

        /** 配置项值 */
        private String value;

        /** 数据类型: STRING, INTEGER, FLOAT, BOOLEAN, JSON等 */
        private String dataType;

        /** 数值单位: ℃, %, Pa, m³/h等 */
        private String unit;

        /** 配置项描述说明 */
        private String description;

        /** 是否可编辑: 0-只读, 1-可编辑 */
        private Integer editable;

        /** 是否必填: 0-可选, 1-必填 */
        private Integer required;

        /** 数据校验规则，正则表达式或校验逻辑 */
        private String validationRule;

        /** 默认值 */
        private String defaultValue;
    }

    /**
     * 获取指定配置项的值
     * @param key 配置项键
     * @return 配置项值，不存在返回null
     */
    public String getConfigValue(String key) {
        if (configItems == null) {
            return null;
        }
        return configItems.stream()
                .filter(item -> key.equals(item.getKey()))
                .map(ConfigItem::getValue)
                .findFirst()
                .orElse(null);
    }

    /**
     * 设置配置项的值
     * 如果配置项不存在则创建，存在则更新
     * @param key 配置项键
     * @param value 配置项值
     */
    public void setConfigValue(String key, String value) {
        if (configItems == null) {
            configItems = new java.util.ArrayList<>();
        }

        // 查找已存在的配置项
        ConfigItem existingItem = configItems.stream()
                .filter(item -> key.equals(item.getKey()))
                .findFirst()
                .orElse(null);

        if (existingItem != null) {
            // 更新现有配置项
            existingItem.setValue(value);
        } else {
            // 创建新的配置项
            ConfigItem newItem = new ConfigItem();
            newItem.setKey(key);
            newItem.setValue(value);
            configItems.add(newItem);
        }
    }

    /**
     * 判断配置是否当前有效
     * 有效条件：状态为启用，且在有效期内
     * @return true-有效，false-无效
     */
    public boolean isValid() {
        Date now = new Date();
        // 状态检查：必须为启用状态（status == 1）
        boolean statusValid = status != null && status == 1;

        // 生效时间检查：如果设置了生效时间，必须在当前时间之前
        boolean effectiveValid = effectiveTime == null || effectiveTime.before(now);

        // 过期时间检查：如果设置了过期时间，必须在当前时间之后
        boolean expireValid = expireTime == null || expireTime.after(now);

        return statusValid && effectiveValid && expireValid;
    }

    /**
     * 获取完整的设备连接地址
     * 格式：protocol://ip:port 或 ip:port
     * @return 连接地址字符串
     */
    public String getDeviceConnectionUrl() {
        if (deviceIp == null || devicePort == null) {
            return null;
        }

        StringBuilder url = new StringBuilder();
        if (protocolType != null) {
            url.append(protocolType.toLowerCase()).append("://");
        }
        url.append(deviceIp).append(":").append(devicePort);
        return url.toString();
    }

    /**
     * 获取设备显示名称
     * 优先返回设备别名，没有则返回设备名称
     * @return 显示名称
     */
    public String getDeviceDisplayName() {
        if (deviceAlias != null && !deviceAlias.trim().isEmpty()) {
            return deviceAlias;
        }
        return deviceName;
    }

    /**
     * 验证设备连接参数是否完整
     * @return true-完整，false-不完整
     */
    public boolean isConnectionParamsValid() {
        return deviceIp != null && !deviceIp.trim().isEmpty() &&
                devicePort != null && devicePort > 0 && devicePort <= 65535;
    }

    /**
     * 获取采集间隔（毫秒）
     * 如果没有设置，返回默认值1000ms
     * @return 采集间隔毫秒数
     */
    public long getCollectionIntervalMillis() {
        if (collectionInterval != null && collectionInterval > 0) {
            return collectionInterval * 1000L;
        }
        return 1000L; // 默认1秒
    }
}