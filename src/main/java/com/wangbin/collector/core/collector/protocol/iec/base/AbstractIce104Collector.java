package com.wangbin.collector.core.collector.protocol.iec.base;

import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.config.CollectorProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.openmuc.j60870.*;
import org.openmuc.j60870.ie.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;

/**
 * 抽象的IEC 104采集器
 */
@Slf4j
public abstract class AbstractIce104Collector extends BaseCollector {

    @Getter
    protected Connection connection;

    protected String host;
    protected int port = 2404;
    protected int commonAddress = 1;
    protected int timeout = 5000;
    protected boolean timeTag = true;
    protected CollectorProperties.Iec104Config iec104Config;

    protected boolean dataTransferStopped = true;

    protected final Map<Integer, CopyOnWriteArrayList<CompletableFuture<Object>>> pendingRequests = new ConcurrentHashMap<>();
    protected final Map<Integer, CacheEntry> valueCache = new ConcurrentHashMap<>();
    protected final Map<Integer, CompletableFuture<Void>> pendingInterrogations = new ConcurrentHashMap<>();
    protected ScheduledExecutorService interrogationScheduler;

    private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1);
    private final long defaultTimeout = 5000;

    protected void initIec104Config(DeviceInfo deviceInfo) {
        this.iec104Config = collectorProperties != null
                ? collectorProperties.getIec104()
                : new CollectorProperties.Iec104Config();

        this.host = deviceInfo.getIpAddress();
        this.port = deviceInfo.getPort() != null ? deviceInfo.getPort() : 2404;

        Map<String, Object> protocolConfig = deviceInfo.getProtocolConfig();
        if (protocolConfig != null) {
            this.commonAddress = (int) protocolConfig.getOrDefault("unitId",
                    iec104Config != null ? iec104Config.getCommonAddress() : 1);
            this.timeout = (int) protocolConfig.getOrDefault("timeout",
                    iec104Config != null ? iec104Config.getTimeout() : 5000);
            this.timeTag = (boolean) protocolConfig.getOrDefault("timeTag", true);
        } else if (iec104Config != null) {
            this.commonAddress = iec104Config.getCommonAddress();
            this.timeout = iec104Config.getTimeout();
        }

        if (interrogationScheduler == null) {
            interrogationScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "iec104-interrogation");
                t.setDaemon(true);
                return t;
            });
        }
    }

    public Connection connect(ConnectionEventListener listener) throws Exception {
        try {
            InetAddress address = InetAddress.getByName(host);
            ClientConnectionBuilder builder = new ClientConnectionBuilder(address);

            connection = builder
                    .setPort(port)
                    .setConnectionTimeout(timeout)
                    .setConnectionEventListener(listener)
                    .build();
            Thread.sleep(200);
            connection.startDataTransfer();
            log.info("IEC 104连接建立成功: {}:{}", host, port);

            maybeTriggerGeneralInterrogation("connect");
            startGeneralInterrogationLoop();
            return connection;
        } catch (Exception e) {
            log.error("IEC 104连接失败: {}:{}", host, port, e);
            throw new Exception("IEC 104连接失败: " + e.getMessage(), e);
        }
    }

    @Override
    public void doDisconnect() {
        if (connection != null) {
            try {
                if (!connection.isStopped()) {
                    connection.stopDataTransfer();
                }
                connection.close();
                log.info("IEC 104连接已断开");
            } catch (Exception e) {
                log.error("断开IEC 104连接失败", e);
            } finally {
                connection = null;
            }
        }
        valueCache.clear();
        pendingInterrogations.values()
                .forEach(f -> f.completeExceptionally(new IOException("connection closed")));
        pendingInterrogations.clear();
        if (interrogationScheduler != null) {
            interrogationScheduler.shutdownNow();
            interrogationScheduler = null;
        }
    }

    protected Map<String, Object> handleResponse(Connection conn, ASdu asdu) {
        Map<String, Object> result = new HashMap<>();

        try {
            ASduType type = asdu.getTypeIdentification();
            CauseOfTransmission cot = asdu.getCauseOfTransmission();

            log.debug("IEC104 ASDU: type={}, cot={}, neg={}",
                    type, cot, asdu.isNegativeConfirm());

            if (type == ASduType.C_IC_NA_1) {
                handleInterrogationAsdu(asdu);
                return result;
            }

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

            for (InformationObject io : ios) {
                int ioa = io.getInformationObjectAddress();
                InformationElement[][] elements = io.getInformationElements();

                Object value = null;
                if (elements != null && elements.length > 0) {
                    value = parseValue(type, elements[0]);
                }
                Object normalized = normalizeValue(value);
                cacheValue(ioa, normalized);

                completeRequest(ioa, normalized);
                if (!isResponse) {
                    handleSpontaneous(ioa, type, normalized, asdu);
                }

                result.put(String.valueOf(ioa), normalized);
            }

        } catch (Exception e) {
            log.error("IEC104 handleResponse failed", e);
        }

        return result;
    }

    private void failAllPending(Exception e) {
        pendingRequests.forEach((k, list) -> list.forEach(f -> f.completeExceptionally(e)));
        pendingRequests.clear();
    }

    private void cacheValue(int ioa, Object value) {
        if (value == null) {
            return;
        }
        valueCache.put(ioa, new CacheEntry(value, System.currentTimeMillis()));
    }

    protected Object getCachedValue(int ioa) {
        CacheEntry entry = valueCache.get(ioa);
        if (entry == null) {
            return null;
        }
        long ttl = iec104Config != null ? iec104Config.getCacheTtl() : 0;
        if (ttl <= 0) {
            return entry.value();
        }
        if (System.currentTimeMillis() - entry.timestamp() <= ttl) {
            return entry.value();
        }
        valueCache.remove(ioa, entry);
        return null;
    }

    private void handleEmptyAsdu(ASdu asdu) {
        if (asdu.getTypeIdentification() == ASduType.C_IC_NA_1 &&
                asdu.getCauseOfTransmission() == CauseOfTransmission.ACTIVATION_TERMINATION) {

            log.info("IEC104 总召完成");
        }
    }

    protected void handleSpontaneous(int ioa, ASduType type, Object value, ASdu asdu) {
        log.debug("IEC104 spontaneous: ioa={}, type={}, value={}",
                ioa, type, value);
    }

    private Object parseValue(ASduType type, InformationElement[] ies) {
        if (ies == null || ies.length == 0) {
            return null;
        }

        InformationElement ie = ies[0];

        return switch (type) {
            case M_SP_NA_1, M_SP_TB_1 -> ((IeSinglePointWithQuality) ie).isOn();
            case M_DP_NA_1, M_DP_TB_1 -> ((IeDoublePointWithQuality) ie)
                    .getDoublePointInformation();
            case M_ME_NA_1, M_ME_TA_1 -> ((IeNormalizedValue) ie)
                    .getUnnormalizedValue();
            case M_ME_NB_1, M_ME_TB_1 -> ((IeScaledValue) ie)
                    .getUnnormalizedValue();
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

    private void completeRequest(int ioAddress, Object value) {
        CopyOnWriteArrayList<CompletableFuture<Object>> futures = pendingRequests.remove(ioAddress);
        if (futures != null) {
            futures.forEach(f -> f.complete(value));
        }
    }

    protected CompletableFuture<Object> registerPendingRequest(int ioAddress) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingRequests.compute(ioAddress, (key, list) -> {
            if (list == null) {
                list = new CopyOnWriteArrayList<>();
            }
            list.add(future);
            return list;
        });

        timeoutScheduler.schedule(() -> {
            if (future.completeExceptionally(new TimeoutException("IEC104 wait timeout for ioa=" + ioAddress))) {
                removePendingFuture(ioAddress, future);
            }
        }, defaultTimeout, TimeUnit.MILLISECONDS);
        return future;
    }

    private void removePendingFuture(int ioAddress, CompletableFuture<Object> future) {
        pendingRequests.computeIfPresent(ioAddress, (key, list) -> {
            list.remove(future);
            return list.isEmpty() ? null : list;
        });
    }

    protected Object normalizeValue(Object value) {
        if (value instanceof Boolean bool) {
            return bool ? 1 : 0;
        }
        if (value instanceof IeDoublePointWithQuality.DoublePointInformation dpi) {
            return dpi.ordinal();
        }
        return value;
    }

    private void handleInterrogationAsdu(ASdu asdu) {
        int qualifier = extractQualifier(asdu);
        CauseOfTransmission cot = asdu.getCauseOfTransmission();
        CompletableFuture<Void> future = pendingInterrogations.get(qualifier);

        switch (cot) {
            case ACTIVATION:
            case ACTIVATION_CON:
                log.info("IEC104 interrogation qualifier {} ACTIVATION", qualifier);
                break;
            case ACTIVATION_TERMINATION:
                if (future != null) {
                    future.complete(null);
                    pendingInterrogations.remove(qualifier);
                }
                log.info("IEC104 interrogation qualifier {} finished", qualifier);
                break;
            default:
                log.debug("IEC104 interrogation cot={} qualifier={}", cot, qualifier);
        }
    }

    private int extractQualifier(ASdu asdu) {
        InformationObject[] ios = asdu.getInformationObjects();
        if (ios == null || ios.length == 0) {
            return 20;
        }
        InformationElement[][] elements = ios[0].getInformationElements();
        if (elements == null || elements.length == 0 || elements[0].length == 0) {
            return 20;
        }
        InformationElement element = elements[0][0];
        if (element instanceof IeQualifierOfInterrogation qoi) {
            return qoi.getValue();
        }
        return 20;
    }

    private void maybeTriggerGeneralInterrogation(String reason) {
        if (iec104Config != null && iec104Config.isGeneralInterrogationOnConnect()) {
            triggerInterrogation(20, reason);
        }
    }

    private void startGeneralInterrogationLoop() {
        if (iec104Config == null || interrogationScheduler == null) {
            return;
        }
        long interval = iec104Config.getGeneralInterrogationInterval();
        if (interval <= 0) {
            return;
        }
        interrogationScheduler.scheduleAtFixedRate(() -> {
            if (!isConnected()) {
                return;
            }
            triggerInterrogation(20, "scheduled");
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    protected void triggerSingleInterrogation(int qualifier, String reason) {
        triggerInterrogation(qualifier, reason);
    }

    private void triggerInterrogation(int qualifier, String reason) {
        if (connection == null) {
            return;
        }
        pendingInterrogations.computeIfAbsent(qualifier, key -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            IeQualifierOfInterrogation qoi = new IeQualifierOfInterrogation(qualifier);
            try {
                connection.interrogation(commonAddress, CauseOfTransmission.ACTIVATION, qoi);
                log.info("Trigger IEC104 interrogation qualifier={} reason={}", qualifier, reason);
            } catch (Exception e) {
                future.completeExceptionally(e);
                pendingInterrogations.remove(key, future);
                log.error("Failed to trigger interrogation qualifier={}", qualifier, e);
            }
            return future;
        });
    }

    protected Optional<Integer> resolveSingleInterrogationQualifier(String raw) {
        if (raw == null || raw.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Integer.parseInt(raw));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    protected record CacheEntry(Object value, long timestamp) {}
}
