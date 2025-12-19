package com.wangbin.collector.core.collector.protocol.iec.base;

import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.openmuc.j60870.*;
import org.openmuc.j60870.ie.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
    protected Connection connection;

    protected String host;
    protected int port = 2404;
    protected int commonAddress = 1;
    protected int timeout = 5000;
    protected boolean timeTag = true;

    /**
     * 数据传输状态
     */
    protected boolean dataTransferStopped = true;

    // =========== 请求处理相关 ===========

    protected final Map<Integer, CompletableFuture<Object>> pendingRequests = new ConcurrentHashMap<>();
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
            this.commonAddress = (int) protocolConfig.getOrDefault("unitId",1);
            this.timeout = (int) protocolConfig.getOrDefault( "timeout", 5000);
            this.timeTag = (boolean) protocolConfig.getOrDefault("timeTag",true);
        }
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
                // 停止数据传输
                if (!connection.isStopped()) {
                    connection.stopDataTransfer();
                }
                // 关闭连接
                connection.close();
                log.info("IEC 104连接已断开");
            } catch (Exception e) {
                log.error("断开IEC 104连接失败", e);
            } finally {
                connection = null;
            }
        }
    }

    // =========== 请求处理相关方法 ===========

    /**
     * 发送读取请求并等待响应
     */
    protected Object waitForResponse(int ioAddress) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        int requestId = generateRequestId(ioAddress);

        pendingRequests.put(requestId, future);

        // 设置超时
        timeoutScheduler.schedule(() -> {
            if (pendingRequests.remove(requestId) != null) {
                future.completeExceptionally(new TimeoutException("请求超时"));
            }
        }, defaultTimeout, TimeUnit.MILLISECONDS);

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 处理收到的ASDU响应
     */
    protected Map<String, Object> handleResponse(Connection conn, ASdu asdu) {
        Map<String, Object> result = new HashMap<>();

        try {
            ASduType type = asdu.getTypeIdentification();
            CauseOfTransmission cot = asdu.getCauseOfTransmission();

            log.debug("IEC104 ASDU: type={}, cot={}, neg={}",
                    type, cot, asdu.isNegativeConfirm());

            // ========= 1️⃣ 错误处理 =========
            if (asdu.isNegativeConfirm() || isErrorCause(cot)) {
                failAllPending(new IOException("IEC104 error: " + cot));
                return result;
            }

            InformationObject[] ios = asdu.getInformationObjects();
            if (ios == null || ios.length == 0) {
                handleEmptyAsdu(asdu);
                return result;
            }

            boolean isResponse =
                    cot == CauseOfTransmission.REQUEST ||
                            cot == CauseOfTransmission.INTERROGATED_BY_STATION;

            // ========= 2️⃣ 逐 IOA 处理 =========
            for (InformationObject io : ios) {
                int ioa = io.getInformationObjectAddress();
                InformationElement[][] elements = io.getInformationElements();

                Object value = null;

                if (elements != null && elements.length > 0) {
                    value = parseValue(type, elements[0]);
                }

                // 统一缓存
                //updateCache(ioa, type, value, asdu);

                if (isResponse) {
                    completeRequest(ioa, value);
                } else {
                    handleSpontaneous(ioa, type, value, asdu);
                }

                result.put(String.valueOf(ioa), value);
            }

        } catch (Exception e) {
            log.error("IEC104 handleResponse failed", e);
        }

        return result;
    }


    private void failAllPending(Exception e) {
        pendingRequests.forEach((k, f) -> f.completeExceptionally(e));
        pendingRequests.clear();
    }
    private void handleEmptyAsdu(ASdu asdu) {
        if (asdu.getTypeIdentification() == ASduType.C_IC_NA_1 &&
                asdu.getCauseOfTransmission() == CauseOfTransmission.ACTIVATION_TERMINATION) {

            log.info("IEC104 总召唤完成");
        }
    }
    protected void handleSpontaneous(int ioa,ASduType type,Object value,ASdu asdu) {
        log.debug("IEC104 spontaneous: ioa={}, type={}, value={}",
                ioa, type, value);

        // TODO: 推送订阅 / 更新缓存 / MQ
    }


    private Object parseValue(ASduType type, InformationElement[] ies) {

        if (ies == null || ies.length == 0) {
            return null;
        }

        InformationElement ie = ies[0];

        return switch (type) {
            // ===== 单点 =====
            case M_SP_NA_1, M_SP_TB_1 -> ((IeSinglePointWithQuality) ie).isOn();
            // ===== 双点 =====
            case M_DP_NA_1, M_DP_TB_1 -> ((IeDoublePointWithQuality) ie)
                    .getDoublePointInformation();
            // ===== 规一化值 =====
            case M_ME_NA_1, M_ME_TA_1 -> ((IeNormalizedValue) ie)
                    .getUnnormalizedValue();
            // ===== 标度化值 =====
            case M_ME_NB_1, M_ME_TB_1 -> ((IeScaledValue) ie)
                    .getUnnormalizedValue();
            // ===== 浮点 =====
            case M_ME_NC_1, M_ME_TC_1 -> ((IeShortFloat) ie).getValue();
            default -> {
                log.debug("Unsupported ASDU type: {}", type);
                yield ie.toString();
            }
        };
    }


    private boolean isErrorCause(CauseOfTransmission cot) {
        return cot == CauseOfTransmission.UNKNOWN_COMMON_ADDRESS_OF_ASDU ||
                cot == CauseOfTransmission.UNKNOWN_INFORMATION_OBJECT_ADDRESS ||
                cot == CauseOfTransmission.UNKNOWN_TYPE_ID ||
                cot == CauseOfTransmission.UNKNOWN_CAUSE_OF_TRANSMISSION;
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
                if (elements[0] instanceof IeSinglePointWithQuality sp) {
                    return sp.isOn();
                }
                break;
            case M_DP_NA_1:
                if (elements[0] instanceof IeDoublePointWithQuality dp) {
                    return dp.getDoublePointInformation().name();
                }
                break;
            case M_ME_NA_1:
                if (elements[0] instanceof IeNormalizedValue nv) {
                    return nv.getUnnormalizedValue();
                }
                break;
            case M_ME_NB_1:
                if (elements[0] instanceof IeScaledValue sv) {
                    return sv.getUnnormalizedValue();
                }
                break;
            case M_ME_NC_1:
                if (elements[0] instanceof IeShortFloat sf) {
                    return sf.getValue();
                }
                break;
        }

        return null;
    }

    /**
     * 完成请求
     */
    private void completeRequest(int ioAddress, Object value) {
        int requestId = generateRequestId(ioAddress);
        CompletableFuture<Object> future = pendingRequests.remove(requestId);

        if (future != null) {
            future.complete(value);
        }
    }

    /**
     * 生成请求ID
     */
    protected int generateRequestId(int ioAddress) {
        return Objects.hash(ioAddress);
    }

}
