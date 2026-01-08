package com.wangbin.collector.core.config.manager;

import com.wangbin.collector.common.domain.entity.ConnectionInfo;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.config.model.ConfigUpdateEvent;
import com.wangbin.collector.core.report.validator.FieldUniquenessValidator;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 配置管理器 - 负责配置的加载、缓存和更新
 *
 * 主要职责：
 * 1. 管理所有配置的缓存
 * 2. 提供线程安全的配置访问接口
 * 3. 处理配置变更事件
 * 4. 协调配置的重新加载
 */
@Slf4j
@Component
public class ConfigManager {

    /**
     * 设备配置缓存 key:设备ID value:设备信息
     */
    private final Map<String, DeviceInfo> deviceCache = new ConcurrentHashMap<>();

    /**
     * 数据点配置缓存 key:设备ID value:数据点列表
     */
    private final Map<String, List<DataPoint>> pointCache = new ConcurrentHashMap<>();

    /**
     * 连接配置缓存 key:设备ID value:连接信息
     */
    private final Map<String, ConnectionInfo> connectionCache = new ConcurrentHashMap<>();

    /**
     * 读写锁，保证配置读写的线程安全
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @Autowired
    private ConfigSyncService configSyncService;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    @Autowired
    private FieldUniquenessValidator fieldUniquenessValidator;

    /**
     * 初始化方法
     */
    @PostConstruct
    public void init() {
        log.info("配置管理器初始化开始...");
        loadAllConfig();
        startConfigSync();
        log.info("配置管理器初始化完成");
    }

    /**
     * 加载所有配置
     */
    private void loadAllConfig() {
        try {
            lock.writeLock().lock();
            log.info("开始加载所有配置...");

            // 从远程服务加载配置
            List<DeviceInfo> devices = configSyncService.loadAllDevices();
            for (DeviceInfo device : devices) {
                String deviceId = device.getDeviceId();

                if (deviceId == null || deviceId.trim().isEmpty()) {
                    log.warn("设备ID为空，跳过设备: {}", device.getDeviceName());
                    continue;
                }

                // 缓存设备信息
                deviceCache.put(deviceId, device);

                try {
                    // 加载设备的数据点
                    List<DataPoint> points = configSyncService.loadDataPoints(deviceId);
                    pointCache.put(deviceId, points != null ? points : new ArrayList<>());

                    // 加载连接配置
                    ConnectionInfo connection = configSyncService.loadConnectionConfig(deviceId);
                    if (connection != null) {
                        connectionCache.put(deviceId, connection);
                    }

                    // 加载采集配置

                    log.debug("设备配置加载成功: {} - {}", deviceId, device.getDeviceName());
                } catch (Exception e) {
                    log.error("加载设备相关配置失败: {}", deviceId, e);
                }
            }

            log.info("配置加载完成，共加载 {} 个设备配置", devices.size());
        } catch (Exception e) {
            log.error("加载所有配置失败", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 根据设备ID获取设备信息
     *
     * @param deviceId 设备ID
     * @return 设备信息，不存在返回null
     */
    public DeviceInfo getDevice(String deviceId) {
        Objects.requireNonNull(deviceId, "设备ID不能为空");

        lock.readLock().lock();
        try {
            return deviceCache.get(deviceId);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取所有设备信息
     *
     * @return 设备信息列表
     */
    public List<DeviceInfo> getAllDevices() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(deviceCache.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取设备的数据点列表
     *
     * @param deviceId 设备ID
     * @return 数据点列表，如果设备不存在返回空列表
     */
    public List<DataPoint> getDataPoints(String deviceId) {
        Objects.requireNonNull(deviceId, "设备ID不能为空");

        lock.readLock().lock();
        try {
            List<DataPoint> points = pointCache.get(deviceId);
            return points != null ? new ArrayList<>(points) : Collections.emptyList();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取单个数据点配置
     *
     * @param deviceId  设备ID
     * @param pointCode 点位编码
     * @return 数据点配置，不存在返回null
     */
    public DataPoint getDataPoint(String deviceId, String pointCode) {
        Objects.requireNonNull(deviceId, "设备ID不能为空");
        Objects.requireNonNull(pointCode, "点位编码不能为空");

        lock.readLock().lock();
        try {
            List<DataPoint> points = pointCache.get(deviceId);
            if (points != null) {
                return points.stream()
                        .filter(p -> pointCode.equals(p.getPointCode()))
                        .findFirst()
                        .orElse(null);
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 根据pointId获取单个数据点配置
     *
     * @param deviceId 设备ID
     * @param pointId  数据点ID
     * @return 数据点配置，不存在返回null
     */
    public DataPoint getDataPointByPointId(String deviceId, String pointId) {
        Objects.requireNonNull(deviceId, "设备ID不能为空");
        Objects.requireNonNull(pointId, "数据点ID不能为空");

        lock.readLock().lock();
        try {
            List<DataPoint> points = pointCache.get(deviceId);
            if (points != null) {
                return points.stream()
                        .filter(p -> pointId.equals(p.getPointId()))
                        .findFirst()
                        .orElse(null);
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取连接配置
     *
     * @param deviceId 设备ID
     * @return 连接信息，不存在返回null
     */
    public ConnectionInfo getConnectionConfig(String deviceId) {
        Objects.requireNonNull(deviceId, "设备ID不能为空");

        lock.readLock().lock();
        try {
            return connectionCache.get(deviceId);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 更新设备配置
     *
     * @param device 设备信息
     * @return 是否更新成功
     */
    public boolean updateDeviceConfig(DeviceInfo device) {
        Objects.requireNonNull(device, "设备信息不能为空");

        try {
            lock.writeLock().lock();

            String deviceId = device.getDeviceId();
            if (deviceId == null || deviceId.trim().isEmpty()) {
                log.error("设备ID为空，无法更新配置");
                return false;
            }

            DeviceInfo oldDevice = deviceCache.get(deviceId);

            // 检查是否需要更新连接
            boolean connectionChanged = false;
            if (oldDevice != null) {
                connectionChanged = isConnectionChanged(oldDevice, device);
            }

            // 更新缓存
            deviceCache.put(deviceId, device);

            // 发布配置更新事件
            ConfigUpdateEvent event = ConfigUpdateEvent.builder()
                    .deviceId(deviceId)
                    .configType("device")
                    .connectionChanged(connectionChanged)
                    .updateTime(new Date())
                    .build();

            eventPublisher.publishEvent(event);
            log.info("设备配置已更新: {} - {}", deviceId, device.getDeviceName());

            return true;
        } catch (Exception e) {
            log.error("更新设备配置失败: {}", device.getDeviceId(), e);
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 更新数据点配置
     *
     * @param deviceId 设备ID
     * @param points   数据点列表
     * @return 是否更新成功
     */
    public boolean updateDataPoints(String deviceId, List<DataPoint> points) {
        Objects.requireNonNull(deviceId, "设备ID不能为空");
        Objects.requireNonNull(points, "数据点列表不能为空");

        try {
            lock.writeLock().lock();

            if (!deviceCache.containsKey(deviceId)) {
                log.warn("设备不存在，无法更新数据点: {}", deviceId);
                return false;
            }

            if (fieldUniquenessValidator != null) {
                fieldUniquenessValidator.validate(deviceId, points);
            }

            pointCache.put(deviceId, new ArrayList<>(points));

            // 发布配置更新事件
            ConfigUpdateEvent event = ConfigUpdateEvent.builder()
                    .deviceId(deviceId)
                    .configType("points")
                    .updateTime(new Date())
                    .build();

            eventPublisher.publishEvent(event);
            log.info("数据点配置已更新: {}, 共 {} 个点", deviceId, points.size());

            return true;
        } catch (Exception e) {
            log.error("更新数据点配置失败: {}", deviceId, e);
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取所有设备ID列表
     *
     * @return 设备ID列表
     */
    public List<String> getAllDeviceIds() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(deviceCache.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 检查设备是否在缓存中
     *
     * @param deviceId 设备ID
     * @return 是否存在
     */
    public boolean containsDevice(String deviceId) {
        Objects.requireNonNull(deviceId, "设备ID不能为空");

        lock.readLock().lock();
        try {
            return deviceCache.containsKey(deviceId);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取缓存统计信息
     *
     * @return 缓存统计信息
     */
    public Map<String, Object> getCacheStats() {
        lock.readLock().lock();
        try {
            Map<String, Object> stats = new HashMap<>();
            stats.put("deviceCount", deviceCache.size());
            stats.put("pointCount", pointCache.values().stream()
                    .mapToInt(List::size)
                    .sum());
            stats.put("connectionCount", connectionCache.size());
            return stats;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 清空所有配置缓存
     */
    public void clearAllCache() {
        try {
            lock.writeLock().lock();
            deviceCache.clear();
            pointCache.clear();
            connectionCache.clear();
            log.info("所有配置缓存已清空");
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 检查连接配置是否发生变化
     *
     * @param oldDevice 旧设备信息
     * @param newDevice 新设备信息
     * @return 连接是否变化
     */
    private boolean isConnectionChanged(DeviceInfo oldDevice, DeviceInfo newDevice) {
        return !Objects.equals(oldDevice.getIpAddress(), newDevice.getIpAddress()) ||
                !Objects.equals(oldDevice.getPort(), newDevice.getPort()) ||
                !Objects.equals(oldDevice.getProtocolType(), newDevice.getProtocolType()) ||
                !Objects.equals(oldDevice.getConnectionType(), newDevice.getConnectionType()) ||
                !Objects.equals(oldDevice.getProtocolConfig(), newDevice.getProtocolConfig()) ||
                !Objects.equals(oldDevice.getConnectionConfig(), newDevice.getConnectionConfig()) ||
                !Objects.equals(oldDevice.getAuthConfig(), newDevice.getAuthConfig());
    }

    /**
     * 启动配置同步监听
     */
    private void startConfigSync() {
        // 启动定时同步任务
        configSyncService.startSyncTask();

        // 注册配置变更监听
        configSyncService.registerConfigListener(this::handleConfigChange);

        log.info("配置同步监听已启动");
    }

    /**
     * 处理配置变更事件
     *
     * @param event 配置更新事件
     */
    private void handleConfigChange(ConfigUpdateEvent event) {
        log.info("收到配置变更通知: {}", event);

        String deviceId = event.getDeviceId();
        String configType = event.getConfigType();

        try {
            // 根据变更类型重新加载配置
            switch (configType) {
                case "device":
                    reloadDeviceConfig(deviceId);
                    break;
                case "points":
                    reloadDataPoints(deviceId);
                    break;
                case "connection":
                    reloadConnectionConfig(deviceId);
                    break;
                case "all":
                    loadAllConfig();
                    break;
                default:
                    log.warn("未知的配置类型: {}", configType);
            }
        } catch (Exception e) {
            log.error("处理配置变更失败: {}", configType, e);
        }
    }

    /**
     * 重新加载设备配置
     *
     * @param deviceId 设备ID
     */
    private void reloadDeviceConfig(String deviceId) {
        if (deviceId == null) {
            log.warn("设备ID为空，跳过设备配置重载");
            return;
        }

        try {
            DeviceInfo device = configSyncService.loadDevice(deviceId);
            if (device != null) {
                updateDeviceConfig(device);
                log.info("设备配置重载成功: {}", deviceId);
            } else {
                // 设备可能被删除
                removeDeviceConfig(deviceId);
                log.info("设备可能已删除，从缓存中移除: {}", deviceId);
            }
        } catch (Exception e) {
            log.error("重新加载设备配置失败: {}", deviceId, e);
        }
    }

    /**
     * 从缓存中移除设备配置
     *
     * @param deviceId 设备ID
     */
    private void removeDeviceConfig(String deviceId) {
        try {
            lock.writeLock().lock();
            deviceCache.remove(deviceId);
            pointCache.remove(deviceId);
            connectionCache.remove(deviceId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 重新加载数据点配置
     *
     * @param deviceId 设备ID
     */
    private void reloadDataPoints(String deviceId) {
        if (deviceId == null) {
            log.warn("设备ID为空，跳过数据点重载");
            return;
        }

        try {
            List<DataPoint> points = configSyncService.loadDataPoints(deviceId);
            if (points != null) {
                updateDataPoints(deviceId, points);
                log.info("数据点配置重载成功: {}", deviceId);
            }
        } catch (Exception e) {
            log.error("重新加载数据点配置失败: {}", deviceId, e);
        }
    }

    /**
     * 重新加载连接配置
     *
     * @param deviceId 设备ID
     */
    private void reloadConnectionConfig(String deviceId) {
        if (deviceId == null) {
            log.warn("设备ID为空，跳过连接配置重载");
            return;
        }

        try {
            ConnectionInfo connection = configSyncService.loadConnectionConfig(deviceId);
            if (connection != null) {
                lock.writeLock().lock();
                try {
                    connectionCache.put(deviceId, connection);
                    log.info("连接配置重载成功: {}", deviceId);
                } finally {
                    lock.writeLock().unlock();
                }
            }
        } catch (Exception e) {
            log.error("重新加载连接配置失败: {}", deviceId, e);
        }
    }
}

