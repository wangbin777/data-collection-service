package com.wangbin.collector.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wangbin.collector.api.application.ConfigDiffCalculator;
import com.wangbin.collector.api.application.ConfigConsoleApplicationService;
import com.wangbin.collector.api.application.ConfigImportExportApplicationService;
import com.wangbin.collector.api.application.LocalDeviceConfigApplicationService;
import com.wangbin.collector.api.controller.dto.ConfigBundle;
import com.wangbin.collector.api.controller.dto.ConfigImportRequest;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.collector.CollectionService;
import com.wangbin.collector.core.collector.runtime.PointRuntimeStateService;
import com.wangbin.collector.core.config.manager.ConfigManager;
import com.wangbin.collector.core.config.manager.ConfigSyncService;
import com.wangbin.collector.core.config.security.SensitiveConfigSanitizer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ConfigController.class)
@Import({ConfigConsoleApplicationService.class,
        LocalDeviceConfigApplicationService.class,
        ConfigImportExportApplicationService.class,
        ConfigDiffCalculator.class,
        SensitiveConfigSanitizer.class})
class ConfigControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private ConfigManager configManager;

    @MockBean
    private ConfigSyncService configSyncService;

    @MockBean
    private CollectionService collectionService;

    @MockBean
    private PointRuntimeStateService pointRuntimeStateService;

    @Test
    void shouldReturnSummary() throws Exception {
        when(configManager.getCacheStats()).thenReturn(Map.of(
                "deviceCount", 1,
                "pointCount", 2,
                "connectionCount", 1,
                "contextCount", 1));
        when(configSyncService.getLastSyncTime()).thenReturn(100L);
        when(configSyncService.getSyncInterval()).thenReturn(1000L);
        when(configSyncService.getServiceId()).thenReturn("collector-1");
        when(configSyncService.getListenerCount()).thenReturn(2);

        mockMvc.perform(get("/api/config/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(200)))
                .andExpect(jsonPath("$.status", is("success")))
                .andExpect(jsonPath("$.data.cacheStats.deviceCount", is(1)))
                .andExpect(jsonPath("$.data.cacheStats.pointCount", is(2)))
                .andExpect(jsonPath("$.data.cacheStats.connectionCount", is(1)))
                .andExpect(jsonPath("$.data.cacheStats.contextCount", is(1)))
                .andExpect(jsonPath("$.data.serviceId", is("collector-1")));
    }

    @Test
    void shouldUpdateDeviceConfig() throws Exception {
        DeviceInfo deviceInfo = new DeviceInfo();
        deviceInfo.setDeviceId("dev-1");
        deviceInfo.setDeviceName("test-device");

        when(configManager.updateDeviceConfig(any(DeviceInfo.class))).thenReturn(true);

        mockMvc.perform(put("/api/config/device/dev-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsBytes(deviceInfo)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("success")));

        verify(configManager).updateDeviceConfig(argThat(device -> "dev-1".equals(device.getDeviceId())));
    }

    @Test
    void shouldImportConfigs() throws Exception {
        ConfigImportRequest request = new ConfigImportRequest();
        ConfigBundle bundle = ConfigBundle.builder()
                .device(new DeviceInfo())
                .build();
        bundle.getDevice().setDeviceId("dev-1");
        request.setBundles(List.of(bundle));

        when(configManager.replaceDeviceContextsAtomically(anyList())).thenReturn(true);

        mockMvc.perform(post("/api/config/import")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsBytes(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.success", is(1)));
    }

    @Test
    void shouldCreateLocalTemporaryDeviceAndStartFromLocalCache() throws Exception {
        Map<String, Object> request = Map.of(
                "device", Map.of(
                        "id", "local-1",
                        "deviceName", "local-device",
                        "protocolType", "MODBUS_TCP"),
                "connection", Map.of(
                        "deviceId", "local-1",
                        "connectionType", "MODBUS_TCP",
                        "host", "127.0.0.1",
                        "port", 502),
                "points", List.of(Map.of(
                        "deviceId", "local-1",
                        "pointCode", "temperature",
                        "address", "40001",
                        "dataType", "FLOAT")),
                "startAfterSave", true);

        when(configManager.saveLocalDeviceConfig(any(DeviceInfo.class), any(DeviceConnection.class), anyList(), eq(false)))
                .thenReturn(true);
        when(collectionService.startLocalDevice("local-1")).thenReturn(true);

        mockMvc.perform(post("/api/config/local/devices")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsBytes(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(200)))
                .andExpect(jsonPath("$.status", is("success")))
                .andExpect(jsonPath("$.data.configSource", is("local")))
                .andExpect(jsonPath("$.data.temporaryConfig", is(true)))
                .andExpect(jsonPath("$.data.started", is(true)));

        verify(configManager).saveLocalDeviceConfig(any(DeviceInfo.class), any(DeviceConnection.class), anyList(), eq(false));
        verify(collectionService).startLocalDevice("local-1");
    }

    @Test
    void shouldRejectDeleteForNonLocalDevice() throws Exception {
        when(configManager.isLocalTemporaryDevice("remote-1")).thenReturn(false);

        mockMvc.perform(delete("/api/config/local/device/remote-1"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").doesNotExist())
                .andExpect(jsonPath("$.status", is("error")));
    }

    @Test
    void shouldReturnNotFoundWhenConnectionDeviceDoesNotExist() throws Exception {
        when(configManager.containsDevice("missing")).thenReturn(false);

        mockMvc.perform(get("/api/config/device/missing/connection"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status", is("error")));
    }

    private DeviceInfo device(String deviceId) {
        DeviceInfo device = new DeviceInfo();
        device.setDeviceId(deviceId);
        device.setDeviceName("local-device");
        device.setProtocolType("MODBUS_TCP");
        return device;
    }

    private DeviceConnection connection(String deviceId) {
        DeviceConnection connection = new DeviceConnection();
        connection.setDeviceId(deviceId);
        connection.setConnectionType("MODBUS_TCP");
        connection.setHost("127.0.0.1");
        connection.setPort(502);
        return connection;
    }

    private DataPoint point(String deviceId) {
        DataPoint point = new DataPoint();
        point.setDeviceId(deviceId);
        point.setPointCode("temperature");
        point.setAddress("40001");
        point.setDataType("FLOAT");
        return point;
    }
}
