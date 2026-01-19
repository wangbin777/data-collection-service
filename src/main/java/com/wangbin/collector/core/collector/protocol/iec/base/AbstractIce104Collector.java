package com.wangbin.collector.core.collector.protocol.iec.base;

import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.config.CollectorProperties;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.openmuc.j60870.*;
import org.openmuc.j60870.ie.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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

    protected final Map<Iec104Key, CopyOnWriteArrayList<CompletableFuture<Object>>> pendingRequests = new ConcurrentHashMap<>();
    protected final Map<Iec104Key, CacheEntry> valueCache = new ConcurrentHashMap<>();
    protected final Map<InterrogationKey, CompletableFuture<Void>> pendingInterrogations = new ConcurrentHashMap<>();
    protected ScheduledExecutorService interrogationScheduler;

    private static final AtomicInteger TIMEOUT_THREAD_COUNTER = new AtomicInteger(0);
    private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1, r -> {
        Thread thread = new Thread(r, "iec104-timeout-" + TIMEOUT_THREAD_COUNTER.incrementAndGet());
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler((t, e) ->
                log.warn("IEC104 timeout scheduler thread {} failed", t.getName(), e));
        return thread;
    });
    private final long defaultTimeout = 5000;

    @PreDestroy
    protected void shutdownSchedulers() {
        timeoutScheduler.shutdownNow();
    }

    protected void initIec104Config(DeviceInfo deviceInfo) {
        this.iec104Config = collectorProperties != null
                ? collectorProperties.getIec104()
                : new CollectorProperties.Iec104Config();

        this.host = deviceInfo.getIpAddress();
        this.port = deviceInfo.getPort() != null ? deviceInfo.getPort() : 2404;

        DeviceConnection connectionConfig = deviceInfo.getConnectionConfig();
        this.commonAddress = connectionConfig.getSlaveId();
        this.timeout = connectionConfig.getTimeout();
        this.timeTag = true;
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
            int commonAddr = asdu.getCommonAddress();

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
                cacheValue(commonAddr, ioa, normalized);

                completeRequest(commonAddr, ioa, normalized);
                if (!isResponse) {
                    handleSpontaneous(commonAddr, ioa, type, normalized, asdu);
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

    private void cacheValue(int commonAddress, int ioa, Object value) {
        if (value == null) {
            return;
        }
        valueCache.put(new Iec104Key(commonAddress, ioa), new CacheEntry(value, System.currentTimeMillis()));
    }

    protected Object getCachedValue(int commonAddress, int ioa) {
        CacheEntry entry = valueCache.get(new Iec104Key(commonAddress, ioa));
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
        valueCache.remove(new Iec104Key(commonAddress, ioa), entry);
        return null;
    }

    private void handleEmptyAsdu(ASdu asdu) {
        if (asdu.getTypeIdentification() == ASduType.C_IC_NA_1 &&
                asdu.getCauseOfTransmission() == CauseOfTransmission.ACTIVATION_TERMINATION) {

            log.info("IEC104 总召完成");
        }
    }

    protected void handleSpontaneous(int commonAddress, int ioa, ASduType type, Object value, ASdu asdu) {
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

    private void completeRequest(int commonAddress, int ioAddress, Object value) {
        Iec104Key key = new Iec104Key(commonAddress, ioAddress);
        CopyOnWriteArrayList<CompletableFuture<Object>> futures = pendingRequests.remove(key);
        if (futures != null) {
            futures.forEach(f -> f.complete(value));
        }
    }

    protected CompletableFuture<Object> registerPendingRequest(int commonAddress, int ioAddress) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        Iec104Key key = new Iec104Key(commonAddress, ioAddress);
        pendingRequests.compute(key, (k, list) -> {
            if (list == null) {
                list = new CopyOnWriteArrayList<>();
            }
            list.add(future);
            return list;
        });

        timeoutScheduler.schedule(() -> {
            if (future.completeExceptionally(
                    new TimeoutException("IEC104 wait timeout for ca/ioa=" + commonAddress + "/" + ioAddress))) {
                removePendingFuture(key, future);
            }
        }, defaultTimeout, TimeUnit.MILLISECONDS);
        return future;
    }

    private void removePendingFuture(Iec104Key key, CompletableFuture<Object> future) {
        pendingRequests.computeIfPresent(key, (k, list) -> {
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
        int commonAddr = asdu.getCommonAddress();
        CauseOfTransmission cot = asdu.getCauseOfTransmission();
        InterrogationKey key = new InterrogationKey(commonAddr, qualifier);
        CompletableFuture<Void> future = pendingInterrogations.get(key);

        switch (cot) {
            case ACTIVATION:
            case ACTIVATION_CON:
                log.info("IEC104 interrogation qualifier {} ACTIVATION", qualifier);
                break;
            case ACTIVATION_TERMINATION:
                if (future != null) {
                    future.complete(null);
                    pendingInterrogations.remove(key);
                }
                log.info("IEC104 interrogation qualifier {} finished for CA {}", qualifier, commonAddr);
                break;
            default:
                log.debug("IEC104 interrogation cot={} qualifier={} ca={}", cot, qualifier, commonAddr);
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
            triggerInterrogation(commonAddress, 20, reason);
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
            triggerInterrogation(commonAddress, 20, "scheduled");
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    protected void triggerSingleInterrogation(int qualifier, String reason) {
        triggerSingleInterrogation(commonAddress, qualifier, reason);
    }

    protected void triggerSingleInterrogation(int targetCommonAddress, int qualifier, String reason) {
        triggerInterrogation(targetCommonAddress, qualifier, reason);
    }

    private void triggerInterrogation(int targetCommonAddress, int qualifier, String reason) {
        if (connection == null) {
            return;
        }
        InterrogationKey key = new InterrogationKey(targetCommonAddress, qualifier);
        pendingInterrogations.computeIfAbsent(key, mapKey -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            IeQualifierOfInterrogation qoi = new IeQualifierOfInterrogation(qualifier);
            try {
                connection.interrogation(targetCommonAddress, CauseOfTransmission.ACTIVATION, qoi);
                log.info("Trigger IEC104 interrogation ca={} qualifier={} reason={}", targetCommonAddress, qualifier, reason);
            } catch (Exception e) {
                future.completeExceptionally(e);
                pendingInterrogations.remove(mapKey, future);
                log.error("Failed to trigger interrogation ca={} qualifier={}", targetCommonAddress, qualifier, e);
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

    public static record Iec104Key(int commonAddress, int ioAddress) {}

    protected record CacheEntry(Object value, long timestamp) {}

    public static record InterrogationKey(int commonAddress, int qualifier) {}
}
