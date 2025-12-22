package com.wangbin.collector.common.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.wangbin.collector.common.domain.entity.DataPoint;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * JSON 数据点加载工具类
 */
@Slf4j
public class JsonDataPointLoader {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        // 配置 ObjectMapper
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }

    /**
     * 从 JSON 文件加载数据点列表
     */
    public static List<DataPoint> loadDataPointsFromJson(String filePath) {
        try {
            log.info("开始从文件加载数据点: {}", filePath);

            // 尝试多种方式读取文件
            InputStream inputStream = null;

            // 1. 尝试从类路径加载
            Resource resource = new ClassPathResource(filePath);
            if (resource.exists()) {
                inputStream = resource.getInputStream();
                log.info("从类路径加载文件: {}", filePath);
            }
            // 2. 尝试从绝对路径加载
            else if (Files.exists(Paths.get(filePath))) {
                inputStream = Files.newInputStream(Paths.get(filePath));
                log.info("从绝对路径加载文件: {}", filePath);
            }
            // 3. 尝试从相对路径加载
            else {
                Path relativePath = Paths.get("src/main/resources/", filePath);
                if (Files.exists(relativePath)) {
                    inputStream = Files.newInputStream(relativePath);
                    log.info("从相对路径加载文件: {}", relativePath);
                } else {
                    throw new IOException("文件不存在: " + filePath);
                }
            }

            // 解析 JSON
            List<Map<String, Object>> pointMaps = objectMapper.readValue(
                    inputStream,
                    new TypeReference<List<Map<String, Object>>>() {}
            );

            // 转换为 DataPoint 对象
            List<DataPoint> dataPoints = convertMapsToDataPoints(pointMaps);

            log.info("成功加载 {} 个数据点", dataPoints.size());
            return dataPoints;

        } catch (Exception e) {
            log.error("加载数据点文件失败: {}", filePath, e);
            throw new RuntimeException("加载数据点文件失败: " + filePath, e);
        }
    }

    /**
     * 从 JSON 字符串加载数据点列表
     */
    public static List<DataPoint> loadDataPointsFromJsonString(String jsonString) {
        try {
            log.info("开始从 JSON 字符串加载数据点");

            List<Map<String, Object>> pointMaps = objectMapper.readValue(
                    jsonString,
                    new TypeReference<List<Map<String, Object>>>() {}
            );

            List<DataPoint> dataPoints = convertMapsToDataPoints(pointMaps);

            log.info("成功加载 {} 个数据点", dataPoints.size());
            return dataPoints;

        } catch (Exception e) {
            log.error("解析 JSON 字符串失败", e);
            throw new RuntimeException("解析 JSON 字符串失败", e);
        }
    }

    /**
     * 将数据点列表转换为 JSON 字符串
     */
    public static String dataPointsToJson(List<DataPoint> dataPoints) {
        try {
            return objectMapper.writeValueAsString(dataPoints);
        } catch (Exception e) {
            log.error("数据点转 JSON 失败", e);
            throw new RuntimeException("数据点转 JSON 失败", e);
        }
    }

    /**
     * 将数据点列表保存到 JSON 文件
     */
    public static void saveDataPointsToJson(List<DataPoint> dataPoints, String filePath) {
        try {
            String json = dataPointsToJson(dataPoints);
            Files.write(Paths.get(filePath), json.getBytes());
            log.info("数据点已保存到文件: {}", filePath);
        } catch (Exception e) {
            log.error("保存数据点到文件失败: {}", filePath, e);
            throw new RuntimeException("保存数据点到文件失败", e);
        }
    }

    /**
     * 将 Map 列表转换为 DataPoint 对象列表
     */
    private static List<DataPoint> convertMapsToDataPoints(List<Map<String, Object>> pointMaps) {
        List<DataPoint> dataPoints = new ArrayList<>();

        for (Map<String, Object> map : pointMaps) {
            try {
                DataPoint point = new DataPoint();

                // 设置基本属性
                if (map.containsKey("id")) point.setId(toLong(map.get("id")));
                if (map.containsKey("pointId")) point.setPointId((String) map.get("pointId"));
                if (map.containsKey("pointCode")) point.setPointCode((String) map.get("pointCode"));
                if (map.containsKey("pointName")) point.setPointName((String) map.get("pointName"));
                if (map.containsKey("deviceId")) point.setDeviceId((String) map.get("deviceId"));
                if (map.containsKey("deviceName")) point.setDeviceName((String) map.get("deviceName"));
                if (map.containsKey("address")) point.setAddress((String) map.get("address"));
                if (map.containsKey("dataType")) point.setDataType((String) map.get("dataType"));
                if (map.containsKey("readWrite")) point.setReadWrite((String) map.get("readWrite"));
                if (map.containsKey("collectionMode")) point.setCollectionMode((String) map.get("collectionMode"));
                //if (map.containsKey("description")) point.setDescription((String) map.get("description"));
                if (map.containsKey("unit")) point.setUnit((String) map.get("unit"));
                if (map.containsKey("unitId")) point.setUnitId(toInteger(map.get("unitId")));
                //if (map.containsKey("defaultValue")) point.setDefaultValue((String) map.get("defaultValue"));

                // 设置数值属性
                if (map.containsKey("scalingFactor")) point.setScalingFactor(toDouble(map.get("scalingFactor")));
                if (map.containsKey("offset")) point.setOffset(toDouble(map.get("offset")));
                if (map.containsKey("minValue")) point.setMinValue(toDouble(map.get("minValue")));
                if (map.containsKey("maxValue")) point.setMaxValue(toDouble(map.get("maxValue")));
                if (map.containsKey("precision")) point.setPrecision(toInteger(map.get("precision")));

                // 设置整数属性
                if (map.containsKey("priority")) point.setPriority(toInteger(map.get("priority")));
                if (map.containsKey("status")) point.setStatus(toInteger(map.get("status")));
                if (map.containsKey("cacheEnabled")) point.setCacheEnabled(toInteger(map.get("cacheEnabled")));
                if (map.containsKey("cacheDuration")) point.setCacheDuration(toInteger(map.get("cacheDuration")));
                if (map.containsKey("alarmEnabled")) point.setAlarmEnabled(toInteger(map.get("alarmEnabled")));
                //if (map.containsKey("stringLength")) point.setStringLength(toInteger(map.get("stringLength")));

                // 设置复杂属性
                if (map.containsKey("alarmRule")) point.setAlarmRule((String) map.get("alarmRule"));
                if (map.containsKey("remark")) point.setRemark((String) map.get("remark"));

                Map<String, Object> additional = point.getAdditionalConfig() != null
                        ? new java.util.HashMap<>(point.getAdditionalConfig())
                        : new java.util.HashMap<>();

                if (map.containsKey("commonAddress")) {
                    additional.put("commonAddress", map.get("commonAddress"));
                }
                if (map.containsKey("registerType")) {
                    additional.put("registerType", map.get("registerType"));
                }
                if (map.containsKey("defaultValue")) {
                    additional.put("defaultValue", map.get("defaultValue"));
                }

                if (!additional.isEmpty()) {
                    point.setAdditionalConfig(additional);
                }

                dataPoints.add(point);

            } catch (Exception e) {
                log.warn("转换数据点失败: {}", map, e);
            }
        }

        return dataPoints;
    }

    /**
     * 类型转换辅助方法
     */
    private static Long toLong(Object obj) {
        if (obj == null) return null;
        if (obj instanceof Integer) return ((Integer) obj).longValue();
        if (obj instanceof Long) return (Long) obj;
        return Long.parseLong(obj.toString());
    }

    private static Integer toInteger(Object obj) {
        if (obj == null) return null;
        if (obj instanceof Integer) return (Integer) obj;
        if (obj instanceof Long) return ((Long) obj).intValue();
        return Integer.parseInt(obj.toString());
    }

    private static Double toDouble(Object obj) {
        if (obj == null) return null;
        if (obj instanceof Double) return (Double) obj;
        if (obj instanceof Float) return ((Float) obj).doubleValue();
        if (obj instanceof Integer) return ((Integer) obj).doubleValue();
        if (obj instanceof Long) return ((Long) obj).doubleValue();
        return Double.parseDouble(obj.toString());
    }

    /**
     * 按寄存器类型分组
     */
    public static Map<String, List<DataPoint>> groupByRegisterType(List<DataPoint> dataPoints) {
        return dataPoints.stream()
                .collect(Collectors.groupingBy(point -> {
                    String address = point.getAddress();
                    if (address == null) return "UNKNOWN";

                    // 根据地址前缀判断寄存器类型
                    if (address.startsWith("0")) return "COIL";
                    if (address.startsWith("1")) return "DISCRETE_INPUT";
                    if (address.startsWith("3")) return "INPUT_REGISTER";
                    if (address.startsWith("4")) return "HOLDING_REGISTER";
                    return "UNKNOWN";
                }));
    }

    /**
     * 打印数据点统计信息
     */
    public static void printStatistics(List<DataPoint> dataPoints) {
        Map<String, List<DataPoint>> grouped = groupByRegisterType(dataPoints);

        log.info("=== 数据点统计信息 ===");
        log.info("总数据点数量: {}", dataPoints.size());

        for (Map.Entry<String, List<DataPoint>> entry : grouped.entrySet()) {
            String type = entry.getKey();
            List<DataPoint> points = entry.getValue();

            log.info("{}: {} 个", type, points.size());

            // 打印该类型的详细地址
            String addresses = points.stream()
                    .map(DataPoint::getAddress)
                    .collect(Collectors.joining(", "));
            log.info("  地址列表: {}", addresses);
        }

        // 按数据类型统计
        Map<String, Long> dataTypeCount = dataPoints.stream()
                .collect(Collectors.groupingBy(
                        DataPoint::getDataType,
                        Collectors.counting()
                ));

        log.info("数据类型分布:");
        dataTypeCount.forEach((type, count) -> {
            log.info("  {}: {} 个", type, count);
        });

        log.info("====================");
    }
}
