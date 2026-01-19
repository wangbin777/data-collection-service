package com.wangbin.collector.core.config.manager;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wangbin.collector.common.domain.entity.ConnectionInfo;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.config.model.ConfigUpdateEvent;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 配置同步服务 - 负责与远程配置中心同步配置
 *
 * 主要功能：
 * 1. 定时从RuoYi管理平台同步配置
 * 2. 配置版本管理
 * 3. 配置变更通知
 * 4. 配置缓存管理
 */
@Slf4j
@Service
public class ConfigSyncService {

    /**
     * RuoYi管理平台地址
     */
    @Value("${collector.config.yun-url:http://localhost:8080}")
    private String runUrl;

    /**
     * 配置同步间隔（毫秒）
     */
    @Value("${collector.config.sync-interval:30000}")
    private long syncInterval;

    /**
     * 采集服务实例ID
     */
    @Value("${collector.config.service-id:collector-1}")
    private String serviceId;

    /**
     * API认证Token
     */
    @Value("${collector.config.api-token:}")
    private String apiToken;

    private final RestTemplate restTemplate;

    /**
     * 配置版本缓存 key:配置类型 value:版本号
     */
    private final Map<String, Long> configVersions = new ConcurrentHashMap<>();

    /**
     * 配置变更监听器列表
     */
    private final List<Consumer<ConfigUpdateEvent>> configListeners = new ArrayList<>();

    /**
     * 设备配置缓存 key:设备编码 value:设备信息
     */
    private final Map<String, DeviceInfo> deviceConfigs = new ConcurrentHashMap<>();

    /**
     * 数据点配置缓存 key:设备编码 value:数据点列表
     */
    private final Map<String, List<DataPoint>> pointConfigs = new ConcurrentHashMap<>();

    /**
     * 构造函数
     */
    public ConfigSyncService() {
        this.restTemplate = new RestTemplate();
    }

    /**
     * 初始化方法
     */
    @PostConstruct
    public void init() {
        log.info("配置同步服务初始化，服务ID: {}, RuoYi地址: {}", serviceId, runUrl);
    }

    /**
     * 启动定时同步任务
     */
    public void startSyncTask() {
        // 首次同步
        try {
            syncAllConfig();
            log.info("首次配置同步完成");
        } catch (Exception e) {
            log.error("首次配置同步失败", e);
        }

        log.info("配置同步任务已启动，同步间隔: {}ms", syncInterval);
    }

    /**
     * 注册配置变更监听器
     *
     * @param listener 配置变更监听器
     */
    public void registerConfigListener(Consumer<ConfigUpdateEvent> listener) {
        if (listener != null) {
            configListeners.add(listener);
            log.debug("配置监听器注册成功，当前监听器数量: {}", configListeners.size());
        }
    }

    /**
     * 取消注册配置变更监听器
     *
     * @param listener 要移除的监听器
     */
    public void unregisterConfigListener(Consumer<ConfigUpdateEvent> listener) {
        if (listener != null) {
            configListeners.remove(listener);
            log.debug("配置监听器取消注册，当前监听器数量: {}", configListeners.size());
        }
    }

    /**
     * 定时同步所有配置
     */
    //@Scheduled(fixedDelayString = "${collector.config.sync-interval:30000}")
    public void syncAllConfig() {
        try {
            log.debug("开始定时同步配置...");

            // 1. 检查配置版本
            Map<String, Long> remoteVersions = getRemoteConfigVersions();

            if (remoteVersions.isEmpty()) {
                log.warn("远程配置版本获取失败，跳过本次同步");
                return;
            }

            // 2. 同步有变化的配置
            boolean hasChanges = false;
            for (Map.Entry<String, Long> entry : remoteVersions.entrySet()) {
                String configType = entry.getKey();
                Long remoteVersion = entry.getValue();
                Long localVersion = configVersions.get(configType);

                if (localVersion == null || !localVersion.equals(remoteVersion)) {
                    log.info("检测到配置变更: {} (本地版本: {}, 远程版本: {})",
                            configType, localVersion, remoteVersion);
                    syncConfigByType(configType);
                    configVersions.put(configType, remoteVersion);
                    hasChanges = true;
                }
            }

            if (hasChanges) {
                log.info("配置同步完成，共检测到 {} 个配置类型变更", configVersions.size());
            } else {
                log.debug("配置未发生变化");
            }
        } catch (Exception e) {
            log.error("配置定时同步失败", e);
        }
    }

    /**
     * 加载所有设备配置
     *
     * @return 设备信息列表
     */
    public List<DeviceInfo> loadAllDevices() {
        try {
            String url = runUrl + "/iot/collector/config/devices?serviceId=" + serviceId;

            ResponseEntity<DeviceInfo[]> response = restTemplate.exchange(
                    url, HttpMethod.GET, createAuthRequest(), DeviceInfo[].class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                List<DeviceInfo> devices = Arrays.asList(response.getBody());

                // 更新本地缓存
                for (DeviceInfo device : devices) {
                    deviceConfigs.put(device.getDeviceId(), device);
                }

                log.info("成功加载 {} 个设备配置", devices.size());
                return devices;
            } else {
                log.warn("加载设备配置失败，HTTP状态码: {}", response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("加载设备配置失败", e);
        }

        return loadMockDevicesFromFile();
    }

    /**
     * 获取文件里面设置的数据
     * 这是一个测试方法 后续可以删除
     * @return
     */
    private List<DeviceInfo> loadMockDevicesFromFile() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            // 从文件读取
            InputStream inputStream = getClass().getClassLoader()
                    .getResourceAsStream("mock/devices.json");

            if (inputStream != null) {
                DeviceInfo[] devices = mapper.readValue(inputStream, DeviceInfo[].class);
                List<DeviceInfo> deviceList = Arrays.asList(devices);

                log.info("从文件加载 {} 个模拟设备配置", deviceList.size());
                return deviceList;
            }
        } catch (Exception e) {
            log.error("从文件加载模拟设备配置失败", e);
        }

        // 如果文件加载失败，返回硬编码数据
        return Collections.emptyList();
    }

    /**
     * 加载单个设备配置
     *
     * @param deviceId 设备ID
     * @return 设备信息，加载失败返回null
     */
    public DeviceInfo loadDevice(String deviceId) {
        try {
            String url = runUrl + "/iot/collector/config/device/" + deviceId;

            ResponseEntity<DeviceInfo> response = restTemplate.exchange(
                    url, HttpMethod.GET, createAuthRequest(), DeviceInfo.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                DeviceInfo device = response.getBody();
                deviceConfigs.put(deviceId, device);
                log.debug("成功加载设备配置: {}", deviceId);
                return device;
            } else {
                log.warn("加载设备配置失败: {}，HTTP状态码: {}", deviceId, response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("加载设备配置失败: {}", deviceId, e);
        }

        return null;
    }

    /**
     * 加载数据点配置
     *
     * @param deviceId 设备ID
     * @return 数据点列表
     */
    public List<DataPoint> loadDataPoints(String deviceId) {
        try {
            String url = runUrl + "/iot/collector/config/points/" + deviceId;

            ResponseEntity<DataPoint[]> response = restTemplate.exchange(
                    url, HttpMethod.GET, createAuthRequest(), DataPoint[].class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                List<DataPoint> points = Arrays.asList(response.getBody());
                pointConfigs.put(deviceId, points);
                log.debug("成功加载设备 {} 的数据点配置，共 {} 个点", deviceId, points.size());
                return points;
            } else {
                log.warn("加载数据点配置失败: {}，HTTP状态码: {}", deviceId, response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("加载数据点配置失败: {}", deviceId, e);
        }

        return Collections.emptyList();
        //return DataUtils.createMockDataPoints(deviceId);
    }

    /**
     * 加载连接配置
     *
     * @param deviceId 设备ID
     * @return 连接信息，加载失败返回null
     */
    public ConnectionInfo loadConnectionConfig(String deviceId) {
        try {
            String url = runUrl + "/iot/collector/config/connection/" + deviceId;

            ResponseEntity<ConnectionInfo> response = restTemplate.exchange(
                    url, HttpMethod.GET, createAuthRequest(), ConnectionInfo.class);

            if (response.getStatusCode() == HttpStatus.OK) {
                log.debug("成功加载连接配置: {}", deviceId);
                return response.getBody();
            } else {
                log.warn("加载连接配置失败: {}，HTTP状态码: {}", deviceId, response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("加载连接配置失败: {}", deviceId, e);
        }

        //return null;
        // 返回模拟数据
        return DataUtils.createMockConnectionInfo(deviceId);
    }



    /**
     * 获取远程配置版本信息
     *
     * @return 配置版本映射
     */
    private Map<String, Long> getRemoteConfigVersions() {
        try {
            String url = runUrl + "/api/collector/config/versions?serviceId=" + serviceId;

            ResponseEntity<Map<String, Long>> response = restTemplate.exchange(
                    url, HttpMethod.GET, createAuthRequest(),
                    new ParameterizedTypeReference<>() {
                    });

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                return response.getBody();
            } else {
                log.warn("获取配置版本失败，HTTP状态码: {}", response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("获取配置版本失败", e);
            return Collections.emptyMap();
        }

        return Collections.emptyMap();
    }

    /**
     * 根据配置类型同步配置
     *
     * @param configType 配置类型（device/points/connection/collection/all）
     */
    private void syncConfigByType(String configType) {
        log.info("开始同步配置类型: {}", configType);

        ConfigUpdateEvent event = ConfigUpdateEvent.builder()
                .configType(configType)
                .updateTime(new Date())
                .build();

        // 根据配置类型设置设备ID
        if (!"all".equals(configType)) {
            // 这里可以根据具体业务逻辑获取受影响的设备ID
            // 暂时设置为null，由监听器自己处理
            event.setDeviceId(null);
        }

        // 通知所有监听器
        notifyConfigListeners(event);

        log.info("配置类型 {} 同步完成", configType);
    }

    /**
     * 通知所有配置监听器
     *
     * @param event 配置更新事件
     */
    private void notifyConfigListeners(ConfigUpdateEvent event) {
        if (configListeners.isEmpty()) {
            log.debug("没有配置监听器需要通知");
            return;
        }

        int successCount = 0;
        int failCount = 0;

        for (Consumer<ConfigUpdateEvent> listener : configListeners) {
            try {
                listener.accept(event);
                successCount++;
            } catch (Exception e) {
                log.error("配置监听器执行失败", e);
                failCount++;
            }
        }

        log.debug("配置变更通知完成，成功: {}，失败: {}", successCount, failCount);
    }

    /**
     * 创建认证请求头
     *
     * @return 包含认证头的HttpEntity
     */
    private HttpEntity<String> createAuthRequest() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-API-Token", getApiToken());
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        return new HttpEntity<>(headers);
    }

    /**
     * 获取API Token
     *
     * @return API Token
     */
    private String getApiToken() {
        if (apiToken != null && !apiToken.trim().isEmpty()) {
            return apiToken.trim();
        }

        // 可以从环境变量、配置文件或认证服务获取
        String envToken = System.getenv("COLLECTOR_API_TOKEN");
        if (envToken != null && !envToken.trim().isEmpty()) {
            return envToken.trim();
        }

        log.warn("API Token未配置，使用默认空字符串");
        return "";
    }

    /**
     * 获取设备配置缓存
     *
     * @return 设备配置缓存映射
     */
    public Map<String, DeviceInfo> getDeviceConfigs() {
        return Collections.unmodifiableMap(deviceConfigs);
    }

    /**
     * 获取数据点配置缓存
     *
     * @return 数据点配置缓存映射
     */
    public Map<String, List<DataPoint>> getPointConfigs() {
        return Collections.unmodifiableMap(pointConfigs);
    }

    /**
     * 清空配置缓存
     */
    public void clearCache() {
        deviceConfigs.clear();
        pointConfigs.clear();
        configVersions.clear();
        log.info("配置缓存已清空");
    }

    /**
     * 手动触发配置同步
     */
    public void triggerManualSync() {
        log.info("手动触发配置同步");
        new Thread(() -> {
            try {
                syncAllConfig();
            } catch (Exception e) {
                log.error("手动配置同步失败", e);
            }
        }, "manual-sync-thread").start();
    }
}