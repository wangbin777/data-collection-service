package com.wangbin.collector.core.report.service;

import com.wangbin.collector.core.report.config.ReportProperties;
import com.wangbin.collector.core.report.model.ReportData;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CacheReportServiceTest {

    @Test
    void splitSnapshotKeepsQualityAndTimestampAligned() {
        ReportProperties props = new ReportProperties();
        props.setMaxPropertiesPerMessage(2);
        props.setMaxPayloadBytes(1024);
        CacheReportService service = new CacheReportService(null, null, props, null);

        ReportData snapshot = new ReportData();
        snapshot.setDeviceId("dev-test");
        snapshot.setTimestamp(1000L);
        snapshot.setPointCode("snapshot");
        snapshot.addMetadata("schemaVersion", props.getSchemaVersion());
        snapshot.addMetadata("seq", 1L);
        snapshot.addProperty("f1", 1.0, 101L, "GOOD");
        snapshot.addProperty("f2", 2.0, 102L, "WARNING");
        snapshot.addProperty("f3", 3.0, 103L, "GOOD");

        List<ReportData> chunks = service.splitSnapshot(snapshot);
        assertEquals(2, chunks.size());

        ReportData chunk1 = chunks.get(0);
        assertEquals(Map.of("f1", 1.0, "f2", 2.0), chunk1.getProperties());
        assertEquals(101L, chunk1.getPropertyTs().get("f1"));
        assertEquals("WARNING", chunk1.getPropertyQuality().get("f2"));
        assertEquals(0, ((Number) chunk1.getMetadata().get("chunkIndex")).intValue());
        assertEquals(2, ((Number) chunk1.getMetadata().get("chunkTotal")).intValue());
        assertNotNull(chunk1.getMetadata().get("batchId"));

        ReportData chunk2 = chunks.get(1);
        assertEquals(Map.of("f3", 3.0), chunk2.getProperties());
        assertEquals(103L, chunk2.getPropertyTs().get("f3"));
        assertEquals(1, ((Number) chunk2.getMetadata().get("chunkIndex")).intValue());
        assertEquals(2, ((Number) chunk2.getMetadata().get("chunkTotal")).intValue());
        assertNotNull(chunk2.getMetadata().get("batchId"));

        assertEquals(chunk1.getMetadata().get("batchId"), chunk2.getMetadata().get("batchId"));
    }
}
