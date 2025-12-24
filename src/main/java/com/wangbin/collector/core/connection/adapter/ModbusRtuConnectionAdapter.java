package com.wangbin.collector.core.connection.adapter;

import com.digitalpetri.modbus.client.ModbusRtuClient;
import com.digitalpetri.modbus.serial.client.SerialPortClientTransport;
import com.wangbin.collector.core.connection.dispatch.MessageBatchDispatcher;
import com.wangbin.collector.core.connection.dispatch.OverflowStrategy;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Modbus RTU 连接适配器，负责串口客户端与批量调度。
 */
@Slf4j
public class ModbusRtuConnectionAdapter extends AbstractConnectionAdapter {

    private ModbusRtuClient client;
    private SerialPortClientTransport transport;
    private MessageBatchDispatcher<ModbusOperation<?>> dispatcher;

    public ModbusRtuConnectionAdapter(ConnectionConfig config) {
        super(config);
    }

    @Override
    protected void doConnect() throws Exception {
        startDispatcher();
        transport = SerialPortClientTransport.create(cfg -> {
            cfg.setSerialPort(config.getStringConfig("serialPort",
                    config.getHost() != null ? config.getHost() : "COM1"));
            cfg.setBaudRate(config.getIntConfig("baudRate", 9600));
            cfg.setDataBits(config.getIntConfig("dataBits", 8));
            cfg.setParity(config.getIntConfig("parity", 0));
            cfg.setStopBits(config.getIntConfig("stopBits", 1));
        });
        client = ModbusRtuClient.create(transport);
        client.connect();
        log.info("Modbus RTU 客户端创建完成: port={} baud={}",
                config.getStringConfig("serialPort", "COM1"),
                config.getIntConfig("baudRate", 9600));
    }

    @Override
    protected void doDisconnect() throws Exception {
        try {
            if (client != null) {
                client.disconnect();
            }
        } catch (Exception e) {
            log.warn("关闭 Modbus RTU 客户端异常", e);
        } finally {
            client = null;
        }
        if (transport != null) {
            try {
                transport.disconnect()
                        .toCompletableFuture()
                        .get(5, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.warn("Modbus RTU 传输关闭被中断", ie);
            } catch (Exception e) {
                log.warn("关闭 Modbus RTU 传输异常", e);
            } finally {
                transport = null;
            }
        }
        stopDispatcher();
    }

    public <T> CompletableFuture<T> submit(ModbusCallable<T> callable) {
        Objects.requireNonNull(callable, "callable");
        if (dispatcher == null) {
            CompletableFuture<T> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("Modbus 调度器未初始化"));
            return failed;
        }
        ModbusOperation<T> operation = new ModbusOperation<>(callable);
        try {
            dispatcher.enqueue(operation);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            operation.fail(e);
        }
        return operation.future;
    }

    public <T> T execute(ModbusCallable<T> callable, long timeoutMillis) throws Exception {
        long effectiveTimeout = timeoutMillis > 0 ? timeoutMillis : getDefaultTimeout();
        try {
            return submit(callable).get(effectiveTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new TimeoutException("Modbus 操作超时(" + effectiveTimeout + "ms)");
        }
    }

    public ModbusRtuClient getClient() {
        return client;
    }

    @Override
    protected void doSend(byte[] data) {
        throw new UnsupportedOperationException("Modbus RTU 连接不支持裸字节发送，请通过 submit() 执行协议操作");
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
        // 由采集器按需触发
    }

    @Override
    protected void doAuthenticate() {
        // 无内置认证
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

    private void processOperations(List<ModbusOperation<?>> operations) {
        if (operations == null || operations.isEmpty()) {
            return;
        }
        for (ModbusOperation<?> operation : operations) {
            if (client == null) {
                operation.fail(new IllegalStateException("Modbus RTU 客户端尚未连接"));
                continue;
            }
            operation.run(client);
        }
    }

    @FunctionalInterface
    public interface ModbusCallable<T> {
        T apply(ModbusRtuClient client) throws Exception;
    }

    private static final class ModbusOperation<T> {
        private final ModbusCallable<T> callable;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        private ModbusOperation(ModbusCallable<T> callable) {
            this.callable = callable;
        }

        private void complete(T value) {
            future.complete(value);
        }

        private void fail(Throwable throwable) {
            future.completeExceptionally(throwable);
        }

        private void run(ModbusRtuClient client) {
            try {
                complete(callable.apply(client));
            } catch (Exception e) {
                fail(e);
            }
        }
    }
}
