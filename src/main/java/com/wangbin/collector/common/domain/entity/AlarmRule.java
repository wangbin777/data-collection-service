package com.wangbin.collector.common.domain.entity;

import lombok.Data;

import java.util.Date;
import java.util.Map;

/**
 * 告警规则实体类
 * 描述：定义数据点的告警规则配置
 */
@Data
public class AlarmRule {

    /**
     * 规则ID
     */
    private String ruleId;

    /**
     * 规则名称
     */
    private String ruleName;

    /**
     * 比较操作符
     * 可选值: >, >=, <, <=, ==, !=
     */
    private String operator;

    /**
     * 阈值
     */
    private Double threshold;

    /**
     * 持续时间（秒）
     * 当数据持续满足告警条件超过该时间才触发告警
     */
    private Integer duration;

    /**
     * 告警级别
     * 可选值: INFO, WARNING, ERROR, CRITICAL
     */
    private String level;

    /**
     * 告警描述
     */
    private String description;

    /**
     * 是否启用
     */
    private Boolean enabled;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 更新时间
     */
    private Date updateTime;

    /**
     * 附加配置信息
     */
    private Map<String, Object> additionalConfig;

    /**
     * 检查值是否满足告警规则
     * @param value 待检查的值
     * @return true: 满足告警条件; false: 不满足
     */
    public boolean checkAlarm(Double value) {
        if (value == null || threshold == null || operator == null) {
            return false;
        }

        switch (operator) {
            case ">":
                return value > threshold;
            case ">=":
                return value >= threshold;
            case "<":
                return value < threshold;
            case "<=":
                return value <= threshold;
            case "==":
                return value.equals(threshold);
            case "!=":
                return !value.equals(threshold);
            default:
                return false;
        }
    }
}