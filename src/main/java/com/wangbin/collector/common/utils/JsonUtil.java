package com.wangbin.collector.common.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * JSON工具类
 */
@Slf4j
public class JsonUtil {

    private JsonUtil() {
        // 工具类，防止实例化
    }

    /**
     * 对象转JSON字符串
     */
    public static String toJsonString(Object object) {
        try {
            return JSON.toJSONString(object);
        } catch (Exception e) {
            log.error("对象转JSON字符串失败", e);
            return null;
        }
    }

    /**
     * 对象转JSON字符串（格式化）
     */
    public static String toJsonStringPretty(Object object) {
        try {
            return JSON.toJSONString(object, JSONWriter.Feature.PrettyFormat);
        } catch (Exception e) {
            log.error("对象转JSON字符串失败", e);
            return null;
        }
    }

    /**
     * JSON字符串转对象
     */
    public static <T> T parseObject(String json, Class<T> clazz) {
        try {
            return JSON.parseObject(json, clazz);
        } catch (Exception e) {
            log.error("JSON字符串转对象失败: {}", json, e);
            return null;
        }
    }

    /**
     * JSON字符串转List
     */
    public static <T> List<T> parseArray(String json, Class<T> clazz) {
        try {
            return JSON.parseArray(json, clazz);
        } catch (Exception e) {
            log.error("JSON字符串转List失败: {}", json, e);
            return null;
        }
    }

    /**
     * JSON字符串转Map
     */
    public static Map<String, Object> parseMap(String json) {
        try {
            return JSON.parseObject(json, new com.alibaba.fastjson2.TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            log.error("JSON字符串转Map失败: {}", json, e);
            return null;
        }
    }

    /**
     * 对象转Map
     */
    public static Map<String, Object> objectToMap(Object object) {
        try {
            String json = toJsonString(object);
            return parseMap(json);
        } catch (Exception e) {
            log.error("对象转Map失败", e);
            return null;
        }
    }

    /**
     * Map转对象
     */
    public static <T> T mapToObject(Map<String, Object> map, Class<T> clazz) {
        try {
            String json = toJsonString(map);
            return parseObject(json, clazz);
        } catch (Exception e) {
            log.error("Map转对象失败", e);
            return null;
        }
    }

    /**
     * 创建JSON对象
     */
    public static JSONObject createJsonObject() {
        return new JSONObject();
    }

    /**
     * 创建JSON数组
     */
    public static JSONArray createJsonArray() {
        return new JSONArray();
    }

    /**
     * 判断是否为有效JSON
     */
    public static boolean isValidJson(String json) {
        try {
            JSON.parse(json);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 判断是否为JSON对象
     */
    public static boolean isJsonObject(String json) {
        try {
            Object obj = JSON.parse(json);
            return obj instanceof JSONObject;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 判断是否为JSON数组
     */
    public static boolean isJsonArray(String json) {
        try {
            Object obj = JSON.parse(json);
            return obj instanceof JSONArray;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 获取JSON值
     */
    public static Object getJsonValue(String json, String key) {
        try {
            JSONObject jsonObject = JSON.parseObject(json);
            return jsonObject.get(key);
        } catch (Exception e) {
            log.error("获取JSON值失败: {}, key: {}", json, key, e);
            return null;
        }
    }

    /**
     * 设置JSON值
     */
    public static String setJsonValue(String json, String key, Object value) {
        try {
            JSONObject jsonObject = JSON.parseObject(json);
            jsonObject.put(key, value);
            return jsonObject.toJSONString();
        } catch (Exception e) {
            log.error("设置JSON值失败: {}, key: {}, value: {}", json, key, value, e);
            return json;
        }
    }

    /**
     * 合并JSON对象
     */
    public static String mergeJson(String json1, String json2) {
        try {
            JSONObject obj1 = JSON.parseObject(json1);
            JSONObject obj2 = JSON.parseObject(json2);
            obj1.putAll(obj2);
            return obj1.toJSONString();
        } catch (Exception e) {
            log.error("合并JSON失败: {}, {}", json1, json2, e);
            return json1;
        }
    }

    /**
     * 深度合并JSON对象
     */
    public static String deepMergeJson(String json1, String json2) {
        try {
            JSONObject obj1 = JSON.parseObject(json1);
            JSONObject obj2 = JSON.parseObject(json2);
            deepMerge(obj1, obj2);
            return obj1.toJSONString();
        } catch (Exception e) {
            log.error("深度合并JSON失败: {}, {}", json1, json2, e);
            return json1;
        }
    }

    private static void deepMerge(JSONObject target, JSONObject source) {
        for (String key : source.keySet()) {
            Object sourceValue = source.get(key);
            Object targetValue = target.get(key);

            if (targetValue instanceof JSONObject && sourceValue instanceof JSONObject) {
                deepMerge((JSONObject) targetValue, (JSONObject) sourceValue);
            } else {
                target.put(key, sourceValue);
            }
        }
    }
}