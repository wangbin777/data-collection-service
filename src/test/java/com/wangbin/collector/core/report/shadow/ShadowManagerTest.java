package com.wangbin.collector.core.report.shadow;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.enums.QualityEnum;
import com.wangbin.collector.core.processor.ProcessResult;
import com.wangbin.collector.core.report.config.ReportProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ShadowManagerTest {

    private ReportProperties properties;

    @BeforeEach
    void setUp() {
        properties = new ReportProperties();
        properties.setMinReportIntervalMs(2000);
        properties.setEventMinIntervalMs(2000);
    }

    @Test
    void changeTriggerRespectsThresholdAndMinInterval() {
        ShadowManager manager = new ShadowManager(properties);
        DataPoint point = createPoint("dev-change", "phaseA", Map.of(
                "reportEnabled", true,
                "reportField", "phaseA",
                "changeThreshold", 2.0,
                "changeMinIntervalMs", 2000L
        ));

        ProcessResult r1 = buildResult(10d, QualityEnum.GOOD.getCode());
        ShadowManager.ShadowUpdateResult first = manager.apply("dev-change", point, r1);
        assertFalse(first.changeTriggered());
        manager.markReportedValues("dev-change", Map.of("phaseA", 10d), Map.of("phaseA", System.currentTimeMillis()));

        ProcessResult r2 = buildResult(13d, QualityEnum.GOOD.getCode());
        ShadowManager.ShadowUpdateResult second = manager.apply("dev-change", point, r2);
        assertTrue(second.changeTriggered());
        manager.markReportedValues("dev-change", Map.of("phaseA", 13d), Map.of("phaseA", System.currentTimeMillis()));

        ProcessResult r3 = buildResult(16d, QualityEnum.GOOD.getCode());
        ShadowManager.ShadowUpdateResult third = manager.apply("dev-change", point, r3);
        assertFalse(third.changeTriggered(), "should be blocked by min interval");

        DeviceShadow shadow = manager.getShadow("dev-change");
        shadow.markChangeTrigger(point.getReportField() + ":change", System.currentTimeMillis() - 5000);

        ProcessResult r4 = buildResult(19d, QualityEnum.GOOD.getCode());
        ShadowManager.ShadowUpdateResult fourth = manager.apply("dev-change", point, r4);
        assertTrue(fourth.changeTriggered(), "should trigger once interval elapsed");
    }

    @Test
    void metadataEventTriggerAndQualityFallbackWork() {
        ShadowManager manager = new ShadowManager(properties);
        DataPoint point = createPoint("dev-event", "ia", Map.of(
                "reportEnabled", true,
                "reportField", "ia",
                "eventEnabled", true,
                "eventMinIntervalMs", 2000L
        ));

        ProcessResult metaEvent = buildResult(5d, QualityEnum.GOOD.getCode());
        metaEvent.setSuccess(true);
        metaEvent.addMetadata("eventTriggered", true);
        metaEvent.addMetadata("eventType", "OVER_LIMIT");
        metaEvent.addMetadata("eventLevel", "CRITICAL");
        metaEvent.addMetadata("eventMessage", "over threshold");

        ShadowManager.ShadowUpdateResult first = manager.apply("dev-event", point, metaEvent);
        assertNotNull(first.eventInfo());
        assertEquals("OVER_LIMIT", first.eventInfo().eventType());

        ShadowManager.ShadowUpdateResult second = manager.apply("dev-event", point, metaEvent);
        assertNull(second.eventInfo(), "event should be throttled by min interval");

        DeviceShadow shadow = manager.getShadow("dev-event");
        String eventKey = point.getReportField() + "|" + first.eventInfo().eventType();
        long past = System.currentTimeMillis() - 5000;
        shadow.markEventTrigger(eventKey, past);
        String signature = first.eventInfo().eventType() + ":" +
                java.util.Objects.hash(first.eventInfo().message(), first.eventInfo().ruleId(), first.eventInfo().ruleName());
        shadow.markEventSignature(signature, past);

        ShadowManager.ShadowUpdateResult third = manager.apply("dev-event", point, metaEvent);
        assertNotNull(third.eventInfo(), "event should fire again after interval");

        ProcessResult badQuality = buildResult(7d, QualityEnum.BAD.getCode());
        badQuality.setSuccess(false);
        ShadowManager.ShadowUpdateResult qualityEvent = manager.apply("dev-event", point, badQuality);
        assertNotNull(qualityEvent.eventInfo());
        assertEquals("QUALITY", qualityEvent.eventInfo().eventType());
    }

    private DataPoint createPoint(String deviceId, String alias, Map<String, Object> config) {
        DataPoint point = new DataPoint();
        point.setDeviceId(deviceId);
        point.setPointId(alias + "-id");
        point.setPointCode(alias + "-code");
        point.setPointAlias(alias);
        point.setAdditionalConfig(new HashMap<>(config));
        return point;
    }

    private ProcessResult buildResult(double value, int quality) {
        ProcessResult result = new ProcessResult();
        result.setSuccess(true);
        result.setProcessedValue(value);
        result.setQuality(quality);
        return result;
    }
}
