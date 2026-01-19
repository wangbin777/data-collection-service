package com.wangbin.collector.core.connection.adapter;

import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.connection.dispatch.MessageBatchDispatcher;
import com.wangbin.collector.core.connection.dispatch.OverflowStrategy;
import lombok.extern.slf4j.Slf4j;
import org.snmp4j.CommunityTarget;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.TransportMapping;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * SNMP 连接适配器，封装会话管理与批量调度。
 */
@Slf4j
public class SnmpConnectionAdapter extends AbstractConnectionAdapter<Snmp> {

    private Snmp snmp;
    private TransportMapping<UdpAddress> transport;
    private Target<UdpAddress> target;
    private MessageBatchDispatcher<SnmpOperation<?>> dispatcher;

    public SnmpConnectionAdapter(DeviceInfo deviceInfo) {
        super(deviceInfo);
    }

    @Override
    protected void doConnect() throws Exception {
        transport = new DefaultUdpTransportMapping();
        transport.listen();
        snmp = new Snmp(transport);
        target = buildTarget();
        startDispatcher();
        log.info("SNMP 会话已建立 host={} port={}", config.getHost(), config.getPort());
    }

    @Override
    protected void doDisconnect() throws Exception {
        stopDispatcher();
        if (snmp != null) {
            try {
                snmp.close();
            } catch (Exception e) {
                log.warn("关闭 SNMP 会话异常", e);
            } finally {
                snmp = null;
            }
        }
        if (transport != null) {
            try {
                transport.close();
            } catch (Exception e) {
                log.warn("关闭 SNMP 传输异常", e);
            } finally {
                transport = null;
            }
        }
        target = null;
    }

    public <T> CompletableFuture<T> submit(SnmpCallable<T> callable) {
        Objects.requireNonNull(callable, "callable");
        if (dispatcher == null) {
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("SNMP 调度器未初始化"));
            return failed;
        }
        SnmpOperation<T> operation = new SnmpOperation<>(callable);
        try {
            dispatcher.enqueue(operation);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            operation.fail(e);
        }
        return operation.future;
    }

    public <T> T execute(SnmpCallable<T> callable, long timeoutMillis) throws Exception {
        long effectiveTimeout = timeoutMillis > 0 ? timeoutMillis : getDefaultTimeout();
        try {
            return submit(callable).get(effectiveTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new TimeoutException("SNMP 操作超时(" + effectiveTimeout + "ms)");
        }
    }

    public Snmp getSnmp() {
        return snmp;
    }

    public Target<UdpAddress> getTarget() {
        return target;
    }

    @Override
    protected void doSend(byte[] data) {
        throw new UnsupportedOperationException("SNMP 适配器不支持裸数据发送");
    }

    @Override
    protected byte[] doReceive() {
        return null;
    }

    @Override
    protected byte[] doReceive(long timeout) {
        return null;
    }

    @Override
    protected void doHeartbeat() {
        // 由采集器根据需要发起探测
    }

    @Override
    protected void doAuthenticate() {
        // SNMPv1/2c 默认无额外认证
    }

    @Override
    public Snmp getClient() {
        return snmp;
    }

    private long getDefaultTimeout() {
        if (config.getReadTimeout() != null && config.getReadTimeout() > 0) {
            return config.getReadTimeout();
        }
        if (config.getTimeout() != null && config.getTimeout() > 0) {
            return config.getTimeout();
        }
        return 3000;
    }

    private Target<UdpAddress> buildTarget() {
        CommunityTarget<UdpAddress> communityTarget = new CommunityTarget<>();
        String community = config.getStringConfig("community", "public");
        communityTarget.setCommunity(new OctetString(community));
        communityTarget.setVersion(parseVersion(config.getStringConfig("snmpVersion", "2c")));
        communityTarget.setRetries(config.getIntConfig("snmpRetries", 1));
        long timeoutMillis = config.getReadTimeout() != null ? config.getReadTimeout() : getDefaultTimeout();
        communityTarget.setTimeout(timeoutMillis);
        String host = config.getHost() != null ? config.getHost() : "127.0.0.1";
        int port = config.getPort() != null ? config.getPort() : 161;
        communityTarget.setAddress(new UdpAddress(host + "/" + port));
        return communityTarget;
    }

    private int parseVersion(String versionText) {
        if (versionText == null) {
            return SnmpConstants.version2c;
        }
        return switch (versionText.trim()) {
            case "1", "v1" -> SnmpConstants.version1;
            case "3", "v3" -> SnmpConstants.version3;
            case "2", "v2", "2c", "v2c" -> SnmpConstants.version2c;
            default -> SnmpConstants.version2c;
        };
    }

    private void startDispatcher() {
        if (dispatcher != null) {
            return;
        }
        int capacity = Math.max(1, config.getMaxPendingMessages() != null ? config.getMaxPendingMessages() : 1024);
        int batchSize = Math.max(1, config.getDispatchBatchSize() != null ? config.getDispatchBatchSize() : 1);
        long flushInterval = config.getDispatchFlushInterval() != null ? config.getDispatchFlushInterval() : 0L;
        dispatcher = new MessageBatchDispatcher<>(capacity, batchSize, flushInterval,
                OverflowStrategy.from(config.getOverflowStrategy()));
        dispatcher.addListener(this::processOperations);
        dispatcher.start();
    }

    private void stopDispatcher() {
        if (dispatcher != null) {
            dispatcher.stop();
            dispatcher = null;
        }
    }

    private void processOperations(List<SnmpOperation<?>> operations) {
        if (operations == null || operations.isEmpty()) {
            return;
        }
        for (SnmpOperation<?> operation : operations) {
            if (snmp == null || target == null) {
                operation.fail(new IllegalStateException("SNMP 会话尚未建立"));
                continue;
            }
            operation.run(snmp, target);
        }
    }

    @FunctionalInterface
    public interface SnmpCallable<T> {
        T apply(Snmp snmp, Target<UdpAddress> target) throws Exception;
    }

    private static final class SnmpOperation<T> {
        private final SnmpCallable<T> callable;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        private SnmpOperation(SnmpCallable<T> callable) {
            this.callable = callable;
        }

        private void complete(T value) {
            future.complete(value);
        }

        private void fail(Throwable throwable) {
            future.completeExceptionally(throwable);
        }

        private void run(Snmp snmp, Target<UdpAddress> target) {
            try {
                complete(callable.apply(snmp, target));
            } catch (Exception e) {
                fail(e);
            }
        }
    }
}
