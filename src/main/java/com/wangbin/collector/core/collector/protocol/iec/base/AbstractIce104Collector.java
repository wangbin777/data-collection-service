package com.wangbin.collector.core.collector.protocol.iec.base;

import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.openmuc.j60870.*;
import org.openmuc.j60870.ie.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 抽象的IEC 104采集器
 */
@Slf4j
public abstract class AbstractIce104Collector extends BaseCollector  {

    // =========== 连接管理相关 ===========

    /**
     * -- GETTER --
     *  获取连接
     */
    @Getter
    private Connection connection;

    protected String host;
    protected int port = 2404;
    protected int commonAddress = 1;
    protected int timeout = 5000;

    /**
     * 数据传输状态
     */
    protected boolean dataTransferStarted = false;

    // =========== 请求处理相关 ===========

    private final Map<Integer, CompletableFuture<Object>> pendingRequests = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1);
    private final long defaultTimeout = 5000;  // 默认超时5秒

    // =========== 配置相关方法 ===========

    /**
     * 初始化配置
     */
    protected void initIec104Config(DeviceInfo deviceInfo) {
        this.host = deviceInfo.getIpAddress();
        this.port = (deviceInfo.getPort() != null ? deviceInfo.getPort() : 2404);

        Map<String, Object> protocolConfig = deviceInfo.getProtocolConfig();
        if (protocolConfig != null) {
            this.commonAddress = getIntValue(protocolConfig, "commonAddress", 1);
            this.timeout = getIntValue(protocolConfig, "timeout", 5000);
        }
    }

    /**
     * 获取整数值
     */
    private int getIntValue(Map<String, Object> map, String key, int defaultValue) {
        if (map.get(key) != null) {
            try {
                return Integer.parseInt(map.get(key).toString());
            } catch (NumberFormatException e) {
                log.warn("配置项{}格式错误，使用默认值{}", key, defaultValue);
            }
        }
        return defaultValue;
    }

    // =========== 连接管理方法 ===========

    /**
     * 建立连接
     */
    public Connection connect(ConnectionEventListener listener) throws Exception {
        try {
            InetAddress address = InetAddress.getByName(host);
            ClientConnectionBuilder builder = new ClientConnectionBuilder(address);

            connection = builder
                    .setPort(port)
                    .setConnectionTimeout(timeout)
                    .setConnectionEventListener(listener)
                    .build();
            // ⭐ 关键：启动数据传输
            Thread.sleep(200);
            connection.startDataTransfer();
            log.info("IEC 104连接建立成功: {}:{}", host, port);
            return connection;
        } catch (Exception e) {
            log.error("IEC 104连接失败: {}:{}", host, port, e);
            throw new Exception("IEC 104连接失败: " + e.getMessage(), e);
        }
    }

    /**
     * 断开连接
     */
    @Override
    public void doDisconnect() {
        if (connection != null) {
            try {
                connection.close();
                log.info("IEC 104连接已断开");
            } catch (Exception e) {
                log.error("断开IEC 104连接失败", e);
            } finally {
                connection = null;
            }
        }
    }

    /**
     * 发送ASDU
     */
    protected void sendASdu(ASdu asdu) throws IOException {
        if (connection == null) {
            throw new IllegalArgumentException("IEC104 连接未建立，禁止发送 ASDU");
        }
        if (!dataTransferStarted) {
            throw new IllegalArgumentException("IEC104 数据传输未启动，禁止发送 ASDU");
        }
        connection.send(asdu);
    }

    // =========== 请求处理相关方法 ===========

    /**
     * 发送读取请求并等待响应
     */
    protected CompletableFuture<Object> sendReadRequest(int ioAddress, String requestType) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        int requestId = generateRequestId(ioAddress, requestType);

        pendingRequests.put(requestId, future);

        // 设置超时
        timeoutScheduler.schedule(() -> {
            if (pendingRequests.remove(requestId) != null) {
                future.completeExceptionally(new TimeoutException("请求超时"));
            }
        }, defaultTimeout, TimeUnit.MILLISECONDS);

        return future;
    }

    /**
     * 处理收到的ASDU响应
     */
    protected void handleResponse(ASdu asdu) {
        try {
            // 解析响应中的信息对象
            InformationObject[] ios = asdu.getInformationObjects();
            if (ios == null || ios.length == 0) {
                return;
            }

            for (InformationObject io : ios) {
                int ioAddress = io.getInformationObjectAddress();
                InformationElement[][] elements = io.getInformationElements();

                if (elements != null && elements.length > 0) {
                    Object value = parseInformationElements(elements[0], asdu.getTypeIdentification());
                    completeRequest(ioAddress, asdu.getTypeIdentification(), value);
                }
            }
        } catch (Exception e) {
            log.error("处理IEC 104响应失败", e);
        }
    }

    /**
     * 解析信息元素
     */
    private Object parseInformationElements(InformationElement[] elements, ASduType aSduType) {
        if (elements == null || elements.length == 0) {
            return null;
        }

        switch (aSduType) {
            case M_SP_NA_1:
                if (elements[0] instanceof IeSinglePointWithQuality) {
                    IeSinglePointWithQuality sp = (IeSinglePointWithQuality) elements[0];
                    return sp.isOn();
                }
                break;
            case M_DP_NA_1:
                if (elements[0] instanceof IeDoublePointWithQuality) {
                    IeDoublePointWithQuality dp = (IeDoublePointWithQuality) elements[0];
                    return dp.getDoublePointInformation().name();
                }
                break;
            case M_ME_NA_1:
                if (elements[0] instanceof IeNormalizedValue) {
                    IeNormalizedValue nv = (IeNormalizedValue) elements[0];
                    return nv.getUnnormalizedValue();
                }
                break;
            case M_ME_NB_1:
                if (elements[0] instanceof IeScaledValue) {
                    IeScaledValue sv = (IeScaledValue) elements[0];
                    return sv.getUnnormalizedValue();
                }
                break;
            case M_ME_NC_1:
                if (elements[0] instanceof IeShortFloat) {
                    IeShortFloat sf = (IeShortFloat) elements[0];
                    return sf.getValue();
                }
                break;
        }

        return null;
    }

    /**
     * 完成请求
     */
    private void completeRequest(int ioAddress, ASduType aSduType, Object value) {
        int requestId = generateRequestId(ioAddress, String.valueOf(aSduType.getId()));
        CompletableFuture<Object> future = pendingRequests.remove(requestId);

        if (future != null) {
            future.complete(value);
        }
    }

    /**
     * 生成请求ID
     */
    private int generateRequestId(int ioAddress, String requestType) {
        return (ioAddress << 16) | requestType.hashCode();
    }

}
