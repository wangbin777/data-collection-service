package com.wangbin.collector.common.enums;

import java.util.Arrays;
import java.util.List;

/**
 * 统一的数据质量戳定义，所有采集/传输链路需统一引用该枚举。
 *
 * <p>质量戳按照现场采集常见状态划分，可以根据 code 持久化，
 * description/userTip 用于展示或日志。</p>
 */
public enum QualityEnum {

    GOOD(100, "GOOD", "数据可靠，采集链路和传感器工作正常"),
    UNCERTAIN(90, "UNCERTAIN", "数据可疑，链路或算法给出警示"),
    WARNING(80, "WARNING", "数据质量降低，需注意"),
    SUSPECT(70, "SUSPECT", "数据可疑,不建议使用"),
    poor(60, "poor", "数据质量较差，不要使用"),
    BAD(0, "BAD", "坏值，禁止使用"),
    NOT_CONNECTED(10, "NOT_CONNECTED", "设备未连接或断链"),
    SENSOR_FAULT(11, "SENSOR_FAULT", "传感器故障或状态异常"),
    OUT_OF_RANGE(12, "OUT_OF_RANGE", "值超出量程或配置范围"),
    CALIBRATION(13, "CALIBRATION", "设备正在校准，数据不可信"),
    MAINTENANCE(14, "MAINTENANCE", "维护模式，暂不产生有效数据"),
    COMMUNICATION_ERROR(15, "COMMUNICATION_ERROR", "通信错误导致读取失败"),
    MANUAL_OVERRIDE(16, "MANUAL_OVERRIDE", "人工干预/手动置数");

    private final int code;
    private final String text;
    private final String description;

    QualityEnum(int code, String text, String description) {
        this.code = code;
        this.text = text;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getText() {
        return text;
    }

    public String getDescription() {
        return description;
    }

    /**
     * 是否为可用数据（GOOD/UNCERTAIN 等可接受状态）
     */
    public boolean isAcceptable() {
        return ACCEPTABLE_CODES.contains(this);
    }

    /**
     * 通过 code 解析 QualityStamp，默认返回 BAD。
     */
    public static QualityEnum fromCode(int code) {
        return Arrays.stream(values())
                .filter(item -> item.code == code)
                .findFirst()
                .orElse(BAD);
    }

    private static final List<QualityEnum> ACCEPTABLE_CODES = List.of(GOOD, UNCERTAIN);
}
