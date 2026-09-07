package com.wangbin.collector.common.web.result;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiResultTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void codeStyleSuccessShouldKeepApiResultJsonShape() throws Exception {
        ApiResult<Map<String, Object>> result = ApiResult.success("操作成功", Map.of("deviceId", "dev-1"));

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(result));

        assertEquals(200, json.get("code").asInt());
        assertFalse(json.has("status"));
        assertEquals("操作成功", json.get("message").asText());
        assertEquals("dev-1", json.get("data").get("deviceId").asText());
        assertTrue(json.has("timestamp"));
    }

    @Test
    void codeStyleFailureShouldKeepApiResultJsonShape() throws Exception {
        ApiResult<Object> result = ApiResult.error(1004, "操作失败");

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(result));

        assertEquals(1004, json.get("code").asInt());
        assertFalse(json.has("status"));
        assertEquals("操作失败", json.get("message").asText());
        assertTrue(json.has("timestamp"));
    }

    @Test
    void statusStyleSuccessShouldExposeCodeAndStatusForFrontendContract() throws Exception {
        ApiResult<Map<String, Object>> result = ApiResult.statusSuccess("配置已保存", Map.of("deviceId", "dev-1"));

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(result));

        assertEquals(200, json.get("code").asInt());
        assertEquals("success", json.get("status").asText());
        assertEquals("配置已保存", json.get("message").asText());
        assertEquals("dev-1", json.get("data").get("deviceId").asText());
        assertTrue(json.has("timestamp"));
    }

    @Test
    void statusStyleFailureShouldKeepLegacyApiResponseJsonShape() throws Exception {
        ApiResult<Map<String, String>> result =
                ApiResult.statusError("请求参数校验失败", Map.of("deviceId", "不能为空"));

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(result));

        assertFalse(json.has("code"));
        assertEquals("error", json.get("status").asText());
        assertEquals("请求参数校验失败", json.get("message").asText());
        assertEquals("不能为空", json.get("data").get("deviceId").asText());
        assertTrue(json.has("timestamp"));
    }

    @Test
    void shouldReadLegacyCommonDomainApiResponseMsgAlias() throws Exception {
        String json = """
                {
                  "code": 0,
                  "msg": "远端配置加载成功",
                  "data": {
                    "deviceId": "dev-1"
                  }
                }
                """;

        ApiResult<Map<String, Object>> result =
                objectMapper.readValue(json, new TypeReference<ApiResult<Map<String, Object>>>() {
                });

        assertTrue(result.isSuccess());
        assertEquals("远端配置加载成功", result.getMessage());
        assertEquals("dev-1", result.getData().get("deviceId"));
    }

    @Test
    void deviceControllerResponseShapeShouldRemainCompatible() throws Exception {
        ApiResult<Object> startResult = ApiResult.deviceSuccess("dev-1", "设备启动成功");

        JsonNode startJson = objectMapper.readTree(objectMapper.writeValueAsString(startResult));

        assertEquals(200, startJson.get("code").asInt());
        assertFalse(startJson.has("data"));
        assertEquals("success", startJson.get("status").asText());
        assertEquals("dev-1", startJson.get("deviceId").asText());
        assertEquals("设备启动成功", startJson.get("message").asText());
        assertTrue(startJson.has("timestamp"));

        ApiResult<List<String>> runningResult = ApiResult.statusSuccess(null, List.of("dev-1")).withCount(1);

        JsonNode runningJson = objectMapper.readTree(objectMapper.writeValueAsString(runningResult));

        assertEquals(200, runningJson.get("code").asInt());
        assertFalse(runningJson.has("message"));
        assertEquals("success", runningJson.get("status").asText());
        assertEquals("dev-1", runningJson.get("data").get(0).asText());
        assertEquals(1, runningJson.get("count").asInt());
        assertTrue(runningJson.has("timestamp"));
    }
}
