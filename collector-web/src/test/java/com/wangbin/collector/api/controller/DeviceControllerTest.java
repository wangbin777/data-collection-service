package com.wangbin.collector.api.controller;

import com.wangbin.collector.api.application.DeviceConsoleApplicationService;
import com.wangbin.collector.core.collector.runtime.DeviceRuntimePhase;
import com.wangbin.collector.core.collector.runtime.DeviceRuntimeSnapshot;
import com.wangbin.collector.core.collector.CollectionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(DeviceController.class)
@Import(DeviceConsoleApplicationService.class)
class DeviceControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private CollectionService collectionService;

    @Test
    void shouldStartDeviceWithLegacyDeviceEnvelopeFields() throws Exception {
        when(collectionService.startDevice("dev-1")).thenReturn(true);

        mockMvc.perform(post("/api/device/dev-1/start"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(200)))
                .andExpect(jsonPath("$.status", is("success")))
                .andExpect(jsonPath("$.message", is("设备启动成功")))
                .andExpect(jsonPath("$.deviceId", is("dev-1")))
                .andExpect(jsonPath("$.timestamp").exists());
    }

    @Test
    void shouldReturnDeviceStartFailureWithLegacyDeviceEnvelopeFields() throws Exception {
        when(collectionService.startDevice("dev-1")).thenReturn(false);

        mockMvc.perform(post("/api/device/dev-1/start"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").doesNotExist())
                .andExpect(jsonPath("$.status", is("error")))
                .andExpect(jsonPath("$.message", is("设备已启动或启动失败")))
                .andExpect(jsonPath("$.deviceId", is("dev-1")));
    }

    @Test
    void shouldReturnRunningDevicesWithCount() throws Exception {
        when(collectionService.getRunningDevices()).thenReturn(List.of("dev-1", "dev-2"));

        mockMvc.perform(get("/api/device/running"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(200)))
                .andExpect(jsonPath("$.status", is("success")))
                .andExpect(jsonPath("$.data[0]", is("dev-1")))
                .andExpect(jsonPath("$.data[1]", is("dev-2")))
                .andExpect(jsonPath("$.count", is(2)));
    }

    @Test
    void shouldTriggerDeviceReloadWithAsyncMessage() throws Exception {
        mockMvc.perform(post("/api/device/reload"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(200)))
                .andExpect(jsonPath("$.status", is("success")))
                .andExpect(jsonPath("$.message", is("已触发设备重新加载")));
    }

    @Test
    void shouldReturnRuntimeSnapshotsWithCount() throws Exception {
        when(collectionService.getDeviceRuntimeSnapshots()).thenReturn(List.of(
                new DeviceRuntimeSnapshot("dev-1", DeviceRuntimePhase.ONLINE,
                        true, false, true, false, 0L, 100L, 1L, 90L, 0, 0L, null, 200L),
                new DeviceRuntimeSnapshot("dev-2", DeviceRuntimePhase.STOPPED,
                        false, false, false, false, 0L, 0L, 0L, 0L, 0, 0L, null, 201L)));

        mockMvc.perform(get("/api/device/runtime"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(200)))
                .andExpect(jsonPath("$.status", is("success")))
                .andExpect(jsonPath("$.data[0].deviceId", is("dev-1")))
                .andExpect(jsonPath("$.data[0].phase", is("ONLINE")))
                .andExpect(jsonPath("$.data[1].deviceId", is("dev-2")))
                .andExpect(jsonPath("$.count", is(2)));
    }

    @Test
    void shouldReturnRunningFlagAtTopLevel() throws Exception {
        when(collectionService.isDeviceRunning("dev-1")).thenReturn(true);

        mockMvc.perform(get("/api/device/dev-1/running"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(200)))
                .andExpect(jsonPath("$.status", is("success")))
                .andExpect(jsonPath("$.deviceId", is("dev-1")))
                .andExpect(jsonPath("$.running", is(true)))
                .andExpect(jsonPath("$.data").doesNotExist());
    }

    @Test
    void shouldReturnDeviceStatusWithStableDtoData() throws Exception {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("deviceId", "dev-1");
        status.put("isRunning", true);
        status.put("isStarting", false);
        status.put("connected", true);
        status.put("reconnecting", false);
        status.put("reconnectNextRetryAt", null);
        status.put("statistics", statistics("dev-1", true, 10, 8, 2, 30, 3, 25L, 80.0D, 2000L));
        status.put("performance", performance());
        when(collectionService.getDeviceStatus("dev-1")).thenReturn(status);

        mockMvc.perform(get("/api/device/dev-1/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(200)))
                .andExpect(jsonPath("$.status", is("success")))
                .andExpect(jsonPath("$.deviceId", is("dev-1")))
                .andExpect(jsonPath("$.data.deviceId", is("dev-1")))
                .andExpect(jsonPath("$.data.isRunning", is(true)))
                .andExpect(jsonPath("$.data.isStarting", is(false)))
                .andExpect(jsonPath("$.data.connected", is(true)))
                .andExpect(jsonPath("$.data.reconnecting", is(false)))
                .andExpect(jsonPath("$.data.reconnectNextRetryAt", nullValue()))
                .andExpect(jsonPath("$.data.statistics.totalExecutions", is(10)))
                .andExpect(jsonPath("$.data.statistics.successRate", is(80.0D)))
                .andExpect(jsonPath("$.data.performance.failureRisk", is("LOW")))
                .andExpect(jsonPath("$.data.performance.recentResponseTimes[1]", is(30)));
    }

    @Test
    void shouldReturnStatisticsWithDynamicDeviceKeysAndStableValueDto() throws Exception {
        Map<String, Map<String, Object>> allStatistics = new LinkedHashMap<>();
        allStatistics.put("dev-1", statistics("dev-1", true, 10, 8, 2, 30, 3, 25L, 80.0D, 2000L));
        allStatistics.put("dev-2", statistics("dev-2", false, 4, 4, 0, 12, 0, 10L, 100.0D, 3000L));
        when(collectionService.getAllStatistics()).thenReturn(allStatistics);

        mockMvc.perform(get("/api/device/statistics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(200)))
                .andExpect(jsonPath("$.status", is("success")))
                .andExpect(jsonPath("$.data.dev-1.totalExecutions", is(10)))
                .andExpect(jsonPath("$.data.dev-1.successRate", is(80.0D)))
                .andExpect(jsonPath("$.data.dev-2.deviceId", is("dev-2")))
                .andExpect(jsonPath("$.data.dev-2.successRate", is(100.0D)));
    }

    private Map<String, Object> statistics(String deviceId, boolean running, int totalExecutions,
                                           int successfulExecutions, int failedExecutions,
                                           int totalPoints, int currentTaskPoints,
                                           long averageExecutionTime, double successRate,
                                           long lastExecutionTime) {
        Map<String, Object> statistics = new LinkedHashMap<>();
        statistics.put("deviceId", deviceId);
        statistics.put("isRunning", running);
        statistics.put("runningDuration", running ? 1000L : 0L);
        statistics.put("totalExecutions", totalExecutions);
        statistics.put("successfulExecutions", successfulExecutions);
        statistics.put("failedExecutions", failedExecutions);
        statistics.put("totalPoints", totalPoints);
        statistics.put("currentTaskPoints", currentTaskPoints);
        statistics.put("averageExecutionTime", averageExecutionTime);
        statistics.put("successRate", successRate);
        statistics.put("lastExecutionTime", lastExecutionTime);
        return statistics;
    }

    private Map<String, Object> performance() {
        Map<String, Object> performance = new LinkedHashMap<>();
        performance.put("deviceId", "dev-1");
        performance.put("totalPoints", 30);
        performance.put("successfulBatches", 8);
        performance.put("failedBatches", 2);
        performance.put("averageBatchTime", 25L);
        performance.put("currentBatchSize", 5);
        performance.put("maxBatchSize", 20);
        performance.put("successRate", 80.0D);
        performance.put("healthScore", 90.0D);
        performance.put("failureRisk", "LOW");
        performance.put("consecutiveFailures", 1);
        performance.put("averageResponseTime", 25L);
        performance.put("recentResponseTimes", List.of(20L, 30L));
        return performance;
    }
}
