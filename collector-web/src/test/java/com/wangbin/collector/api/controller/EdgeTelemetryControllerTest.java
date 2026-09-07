package com.wangbin.collector.api.controller;

import com.wangbin.collector.core.collector.edge.EdgeProtocolType;
import com.wangbin.collector.core.collector.edge.EdgeTelemetryBatch;
import com.wangbin.collector.core.collector.edge.EdgeTelemetryIngressResult;
import com.wangbin.collector.core.collector.edge.EdgeTelemetryIngressService;
import com.wangbin.collector.core.collector.edge.EdgeTelemetrySample;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class EdgeTelemetryControllerTest {

    @Test
    void shouldKeepRequestJsonContractAndMapToCoreModel() throws Exception {
        EdgeTelemetryIngressService ingressService = mock(EdgeTelemetryIngressService.class);
        when(ingressService.ingest(any(EdgeTelemetryBatch.class)))
                .thenReturn(new EdgeTelemetryIngressResult("gateway-1", "v1", 1, 0, 0, List.of()));
        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new EdgeTelemetryController(ingressService)).build();

        mockMvc.perform(post("/api/edge/telemetry")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "gatewayId": "gateway-1",
                                  "protocol": "PROFINET",
                                  "configVersion": "v1",
                                  "items": [
                                    {
                                      "deviceId": "dev-1",
                                      "pointRef": "temperature",
                                      "value": 12.5,
                                      "quality": 100,
                                      "timestamp": 123456789,
                                      "sequence": 7
                                    }
                                  ]
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.status").value("success"))
                .andExpect(jsonPath("$.data.gatewayId").value("gateway-1"))
                .andExpect(jsonPath("$.data.configVersion").value("v1"))
                .andExpect(jsonPath("$.data.acceptedCount").value(1));

        ArgumentCaptor<EdgeTelemetryBatch> captor = ArgumentCaptor.forClass(EdgeTelemetryBatch.class);
        verify(ingressService).ingest(captor.capture());
        EdgeTelemetryBatch batch = captor.getValue();
        EdgeTelemetrySample sample = batch.items().get(0);
        assertEquals("gateway-1", batch.gatewayId());
        assertEquals(EdgeProtocolType.PROFINET, batch.protocol());
        assertEquals("v1", batch.configVersion());
        assertEquals("dev-1", sample.deviceId());
        assertEquals("temperature", sample.pointRef());
        assertEquals(12.5d, sample.value());
        assertEquals(100, sample.quality());
        assertEquals(123456789L, sample.timestamp());
        assertEquals(7L, sample.sequence());
    }
}
