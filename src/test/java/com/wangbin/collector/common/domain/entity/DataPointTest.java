package com.wangbin.collector.common.domain.entity;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataPointTest {

    @Test
    void getReportFieldPrefersConfiguredField() {
        DataPoint point = new DataPoint();
        point.setPointAlias("current");
        Map<String, Object> config = new HashMap<>();
        config.put("reportEnabled", true);
        config.put("reportField", " currentA ");
        point.setAdditionalConfig(config);

        assertEquals("currentA", point.getReportField());
        assertTrue(point.isReportEnabled());
    }

    @Test
    void aliasFallbackAndTrimWorks() {
        DataPoint point = new DataPoint();
        point.setPointAlias(" voltage ");
        Map<String, Object> config = new HashMap<>();
        config.put("reportEnabled", true);
        point.setAdditionalConfig(config);

        assertEquals("voltage", point.getReportField());
        assertTrue(point.isReportEnabled());
    }

    @Test
    void missingAliasMeansRawOnly() {
        DataPoint point = new DataPoint();
        point.setPointAlias("   ");
        Map<String, Object> config = new HashMap<>();
        config.put("reportEnabled", true);
        point.setAdditionalConfig(config);

        assertNull(point.getReportField());
        assertFalse(point.isReportEnabled());
    }

    @Test
    void disablingReportFlagTakesEffect() {
        DataPoint point = new DataPoint();
        point.setPointAlias("temp");
        Map<String, Object> config = new HashMap<>();
        config.put("reportEnabled", false);
        point.setAdditionalConfig(config);

        assertEquals("temp", point.getReportField());
        assertFalse(point.isReportEnabled());
    }
}