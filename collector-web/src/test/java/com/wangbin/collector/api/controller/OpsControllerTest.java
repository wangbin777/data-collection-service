package com.wangbin.collector.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wangbin.collector.api.application.OpsConsoleApplicationService;
import com.wangbin.collector.monitor.alert.AlarmAcknowledgement;
import com.wangbin.collector.monitor.alert.AlarmAcknowledgementRequest;
import com.wangbin.collector.monitor.alert.AlarmAcknowledgementService;
import com.wangbin.collector.monitor.log.OperationLogger;
import com.wangbin.collector.monitor.network.NetworkDiagnosticRequest;
import com.wangbin.collector.monitor.network.NetworkDiagnosticResult;
import com.wangbin.collector.monitor.network.NetworkDiagnosticService;
import com.wangbin.collector.monitor.network.NetworkDiagnosticType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OpsController.class)
@Import(OpsConsoleApplicationService.class)
class OpsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private OperationLogger operationLogger;

    @MockBean
    private AlarmAcknowledgementService alarmAcknowledgementService;

    @MockBean
    private NetworkDiagnosticService networkDiagnosticService;

    @Test
    void shouldQuerySanitizedOperationLogs() throws Exception {
        when(operationLogger.query("ERROR", null, "连接", 20))
                .thenReturn(List.of(new OperationLogger.OperationLogEntry(
                        1_000L, "ERROR", "测试日志", "测试线程", "设备连接失败")));
        when(operationLogger.size()).thenReturn(1);

        mockMvc.perform(get("/api/ops/logs")
                        .param("level", "ERROR")
                        .param("keyword", "连接")
                        .param("limit", "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code", is(200)))
                .andExpect(jsonPath("$.status", is("success")))
                .andExpect(jsonPath("$.data.count", is(1)))
                .andExpect(jsonPath("$.data.items[0].message", is("设备连接失败")));
    }

    @Test
    void shouldAcknowledgeAlarmWithLocalOperator() throws Exception {
        AlarmAcknowledgement acknowledgement = new AlarmAcknowledgement(
                "alarm-001", "本机控制台", 2_000L, "已处理", "request-001");
        when(alarmAcknowledgementService.acknowledge(
                eq("alarm-001"), eq("本机控制台"), any(AlarmAcknowledgementRequest.class)))
                .thenReturn(acknowledgement);

        mockMvc.perform(post("/api/ops/alarms/alarm-001/acknowledge")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsBytes(
                                new AlarmAcknowledgementRequest("已处理", "request-001"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.alarmId", is("alarm-001")))
                .andExpect(jsonPath("$.data.operator", is("本机控制台")));
    }

    @Test
    void shouldExecuteRestrictedNetworkDiagnostic() throws Exception {
        NetworkDiagnosticResult result = new NetworkDiagnosticResult(
                NetworkDiagnosticType.PING,
                null,
                "127.0.0.1",
                "127.0.0.1",
                null,
                true,
                8L,
                "目标可达",
                List.of("往返耗时 8 毫秒"),
                3_000L);
        when(networkDiagnosticService.diagnose(any(NetworkDiagnosticRequest.class))).thenReturn(result);

        mockMvc.perform(post("/api/ops/network/diagnose")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "type": "PING",
                                  "target": "127.0.0.1",
                                  "timeoutMs": 3000
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.reachable", is(true)))
                .andExpect(jsonPath("$.data.message", is("目标可达")));

        verify(networkDiagnosticService).diagnose(any(NetworkDiagnosticRequest.class));
    }
}
