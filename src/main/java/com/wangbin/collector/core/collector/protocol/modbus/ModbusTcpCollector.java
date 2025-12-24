package com.wangbin.collector.core.collector.protocol.modbus;

import com.digitalpetri.modbus.pdu.*;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.enums.DataType;
import com.wangbin.collector.core.collector.protocol.modbus.base.AbstractModbusCollector;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusAddress;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusRequestBuilder;
import com.wangbin.collector.core.collector.protocol.modbus.domain.RegisterType;
import com.wangbin.collector.core.collector.protocol.modbus.plan.ModbusReadPlan;
import com.wangbin.collector.core.collector.protocol.modbus.plan.PointOffset;
import com.wangbin.collector.core.collector.protocol.modbus.utils.ModbusUtils;
import com.wangbin.collector.core.config.CollectorProperties;
import com.wangbin.collector.core.connection.adapter.ConnectionAdapter;
import com.wangbin.collector.core.connection.adapter.ModbusTcpConnectionAdapter;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Modbus TCP閲囬泦鍣紙绠€鍖栫増锛?
 */
@Slf4j
@Component
public class ModbusTcpCollector extends AbstractModbusCollector {

    private ModbusTcpConnectionAdapter connectionAdapter;
    private ConnectionConfig managedConnectionConfig;
    private String host;
    private int port;
    private static final int MAX_WRITE_REGISTERS = 123;
    private static final int MAX_WRITE_COILS = 1968;
    @Override
    public String getCollectorType() {
        return "ModbusTCP";
    }

    @Override
    public String getProtocolType() {
        return "MODBUS_TCP";
    }

    @Override
    protected void doConnect() throws Exception {
        log.info("寮€濮嬪缓绔婱odbus TCP杩炴帴: {}", deviceInfo.getDeviceId());
        this.managedConnectionConfig = buildConnectionConfig();
        ConnectionAdapter adapter = connectionManager.createConnection(managedConnectionConfig);
        connectionManager.connect(deviceInfo.getDeviceId());
        if (!(adapter instanceof ModbusTcpConnectionAdapter modbusAdapter)) {
            throw new IllegalStateException("Modbus TCP连接适配器类型不匹配");
        }
        this.connectionAdapter = modbusAdapter;
        this.timeout = managedConnectionConfig.getReadTimeout() != null
                ? managedConnectionConfig.getReadTimeout()
                : managedConnectionConfig.getTimeout();
        log.info("Modbus TCP杩炴帴寤虹珛鎴愬姛: {}:{}", host, port);
    }

    @Override
    protected void doDisconnect() throws Exception {
        if (connectionManager != null && deviceInfo != null) {
            connectionManager.removeConnection(deviceInfo.getDeviceId());
        }
        connectionAdapter = null;
        managedConnectionConfig = null;
        registerCache.clear();
        log.info("Modbus TCP杩炴帴宸叉柇寮€");
    }

    @Override
    protected Object doReadPoint(DataPoint point) throws Exception {
        String address = point.getAddress();
        if (address == null || address.isEmpty()) {
            throw new IllegalArgumentException("鐐逛綅鍦板潃涓嶈兘涓虹┖");
        }

        ModbusAddress modbusAddress = parseModbusAddress(address);
        int unitId = resolveUnitId(point);
        return switch (modbusAddress.getRegisterType()) {
            case COIL -> readCoil(unitId,modbusAddress);
            case DISCRETE_INPUT -> readDiscreteInput(unitId,modbusAddress);
            case HOLDING_REGISTER -> readHoldingRegister(unitId,modbusAddress, point.getDataType());
            case INPUT_REGISTER -> readInputRegister(unitId,modbusAddress, point.getDataType());
            default -> throw new IllegalArgumentException("涓嶆敮鎸佺殑瀵勫瓨鍣ㄧ被鍨? " + modbusAddress.getRegisterType());
        };
    }

    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) {

        Map<String, Object> results = new HashMap<>();

        for (ModbusReadPlan plan : readPlans) {
            try {
                byte[] raw = executeReadPlan(plan);

                if (plan.getRegisterType() == RegisterType.COIL ||
                        plan.getRegisterType() == RegisterType.DISCRETE_INPUT) {
                    List<Boolean> boolValues = ModbusUtils.getCoilValues(raw, plan.getQuantity());
                    for (PointOffset po : plan.getPointOffsets()) {
                        Boolean value = null;
                        int offset = po.getOffset();
                        if (offset >= 0 && offset < boolValues.size()) {
                            value = boolValues.get(offset);
                        }
                        results.put(po.getPointId(), value);
                    }
                } else {
                    for (PointOffset po : plan.getPointOffsets()) {
                        Object value = ModbusUtils.parseValue(
                                raw,
                                po.getOffset(),
                                DataType.valueOf(po.getDataType())
                        );
                        results.put(po.getPointId(), value);
                    }
                }

            } catch (Exception e) {
                log.error("ReadPlan 鎵ц澶辫触: unitId={}, type={}, addr={}",
                        plan.getUnitId(),
                        plan.getRegisterType(),
                        plan.getStartAddress(),
                        e
                );

                for (PointOffset po : plan.getPointOffsets()) {
                    results.put(po.getPointId(), null);
                }
            }
        }

        return results;
    }

    /**
     * 鎵ц璁″垝
     * @param plan
     * @return
     * @throws Exception
     */
    private byte[] executeReadPlan(ModbusReadPlan plan) throws Exception {

        return switch (plan.getRegisterType()) {

            case COIL -> {
                byte[] coils = executeWithClient(client -> client.readCoilsAsync(
                                plan.getUnitId(),
                                new ReadCoilsRequest(plan.getStartAddress(), plan.getQuantity()))
                        .toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS).coils());
                yield coils;
            }

            case DISCRETE_INPUT -> {
                byte[] inputs = executeWithClient(client -> client.readDiscreteInputsAsync(
                                plan.getUnitId(),
                                new ReadDiscreteInputsRequest(plan.getStartAddress(), plan.getQuantity()))
                        .toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS).inputs());
                yield inputs;
            }

            case HOLDING_REGISTER -> {
                byte[] registers = executeWithClient(client -> client.readHoldingRegistersAsync(
                                plan.getUnitId(),
                                new ReadHoldingRegistersRequest(plan.getStartAddress(), plan.getQuantity()))
                        .toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS).registers());
                yield registers;
            }

            case INPUT_REGISTER -> {
                byte[] registers = executeWithClient(client -> client.readInputRegistersAsync(
                                plan.getUnitId(),
                                new ReadInputRegistersRequest(plan.getStartAddress(), plan.getQuantity()))
                        .toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS).registers());
                yield registers;
            }
        };
    }

    /**
     * 杩斿洖缁撴灉
     * @param response
     * @return
     */
    private byte[] extractRaw(Object response) {

        if (response instanceof ReadCoilsResponse r) {
            return r.coils();
        }

        if (response instanceof ReadDiscreteInputsResponse r) {
            return r.inputs();
        }

        if (response instanceof ReadHoldingRegistersResponse r) {
            return r.registers();
        }

        if (response instanceof ReadInputRegistersResponse r) {
            return r.registers();
        }

        throw new IllegalArgumentException(
                "Unsupported Modbus response type: " + response.getClass()
        );
    }



    @Override
    protected boolean doWritePoint(DataPoint point, Object value) throws Exception {
        String address = point.getAddress();
        if (address == null || address.isEmpty()) {
            throw new IllegalArgumentException("鐐逛綅鍦板潃涓嶈兘涓虹┖");
        }

        ModbusAddress modbusAddress = parseModbusAddress(address);
        int unitId = resolveUnitId(point);
        return switch (modbusAddress.getRegisterType()) {
            case COIL -> writeCoil(unitId,modbusAddress, (Boolean) value);
            case HOLDING_REGISTER -> writeHoldingRegister(unitId,modbusAddress, value, point.getDataType());
            default -> throw new IllegalArgumentException("璇ュ瘎瀛樺櫒绫诲瀷涓嶆敮鎸佸啓鍏? " + modbusAddress.getRegisterType());
        };
    }

    @Override
    protected Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) throws Exception {
        Map<String, Boolean> results = new HashMap<>();
        Map<BatchKey, List<WriteEntry>> grouped = new LinkedHashMap<>();

        for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
            DataPoint point = entry.getKey();
            Object value = entry.getValue();

            try {
                ModbusAddress address = parseModbusAddress(point.getAddress());
                RegisterType type = address.getRegisterType();

                if (type != RegisterType.COIL && type != RegisterType.HOLDING_REGISTER) {
                    boolean success = doWritePoint(point, value);
                    results.put(point.getPointId(), success);
                    continue;
                }

                int unitId = resolveUnitId(point);
                int registerCount = DataType.fromString(point.getDataType()).getRegisterCount();
                BatchKey key = new BatchKey(unitId, type);
                grouped.computeIfAbsent(key, k -> new ArrayList<>())
                        .add(new WriteEntry(point, value, address.getAddress(), registerCount));
            } catch (Exception e) {
                log.error("瑙ｆ瀽鍐欏叆鐐逛綅澶辫触: {}", point.getPointName(), e);
                results.put(point.getPointId(), false);
            }
        }

        for (Map.Entry<BatchKey, List<WriteEntry>> batch : grouped.entrySet()) {
            List<WriteEntry> entries = batch.getValue();
            entries.sort(Comparator.comparingInt(WriteEntry::address));
            processWriteBatch(batch.getKey(), entries, results);
        }

        return results;
    }

    @Override
    protected Map<String, Object> doGetDeviceStatus() {
        Map<String, Object> status = getBaseDeviceStatus("Modbus TCP");
        status.put("host", host);
        status.put("port", port);
        status.put("unitIds", collectUnitIds());

        // 娴嬭瘯杩炴帴
        try {
            boolean connected = testConnection();
            status.put("deviceConnected", connected);
        } catch (Exception e) {
            status.put("deviceConnected", false);
            status.put("connectionError", e.getMessage());
        }

        return status;
    }

    @Override
    protected Object doExecuteCommand(int unitId,String command, Map<String, Object> params) throws Exception {
        return switch (command.toUpperCase()) {
            case "READ_MULTIPLE_REGISTERS" -> executeReadMultipleRegisters(unitId,params);
            case "WRITE_MULTIPLE_REGISTERS" -> executeWriteMultipleRegisters(unitId,params);
            case "READ_COILS" -> executeReadCoils(unitId,params);
            case "WRITE_COILS" -> executeWriteCoils(unitId,params);
            case "DIAGNOSTIC" -> executeDiagnostic(unitId,params);
            default -> throw new IllegalArgumentException("涓嶆敮鎸佺殑Modbus鍛戒护: " + command);
        };
    }

    // =============== Modbus鎿嶄綔瀹炵幇 ===============

    private Boolean readCoil(int unitId,ModbusAddress address) throws Exception {
        return executeWithClient(client -> {
            CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(unitId,
                    new ReadCoilsRequest(address.getAddress(), 1));
            ReadCoilsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            return ModbusUtils.parseCoilValue(response.coils(), 0);
        });
    }

    private Boolean readDiscreteInput(int unitId,ModbusAddress address) throws Exception {
        return executeWithClient(client -> {
            CompletionStage<ReadDiscreteInputsResponse> future = client.readDiscreteInputsAsync(unitId,
                    new ReadDiscreteInputsRequest(address.getAddress(), 1));
            ReadDiscreteInputsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            return ModbusUtils.parseCoilValue(response.inputs(), 0);
        });
    }

    private Object readHoldingRegister(int unitId,ModbusAddress address, String dataType) throws Exception {
        int registerCount = DataType.fromString(dataType).getRegisterCount();
        return executeWithClient(client -> {
            CompletionStage<ReadHoldingRegistersResponse> future = client.readHoldingRegistersAsync(
                    unitId,
                    new ReadHoldingRegistersRequest(address.getAddress(), registerCount)
            );
            ReadHoldingRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            return ModbusUtils.parseRegisterValue(response.registers(), dataType);
        });
    }

    private Object readInputRegister(int unitId,ModbusAddress address, String dataType) throws Exception {
        int registerCount = DataType.fromString(dataType).getRegisterCount();
        return executeWithClient(client -> {
            CompletionStage<ReadInputRegistersResponse> future = client.readInputRegistersAsync(
                    unitId,
                    new ReadInputRegistersRequest(address.getAddress(), registerCount)
            );
            ReadInputRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            return ModbusUtils.parseRegisterValue(response.registers(), dataType);
        });
    }

    private boolean writeCoil(int unitId,ModbusAddress address, boolean value) throws Exception {
        return executeWithClient(client -> {
            CompletionStage<WriteSingleCoilResponse> future = client.writeSingleCoilAsync(
                    unitId,
                    new WriteSingleCoilRequest(address.getAddress(), value)
            );
            WriteSingleCoilResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            return response != null;
        });
    }

    private boolean writeHoldingRegister(int unitId,ModbusAddress address, Object value, String dataType) throws Exception {
        int registerCount = DataType.fromString(dataType).getRegisterCount();
        short[] registers = ModbusUtils.valueToRegisters(value, dataType, java.nio.ByteOrder.BIG_ENDIAN);

        if (registerCount == 1) {
            WriteSingleRegisterRequest request = new WriteSingleRegisterRequest(address.getAddress(), registers[0]);
            try {
                return executeWithClient(client -> {
                    CompletionStage<WriteSingleRegisterResponse> future = client.writeSingleRegisterAsync(unitId, request);
                    WriteSingleRegisterResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                    return response != null;
                });
            } finally {
                ReferenceCountUtil.release(request);
            }
        } else {
            WriteMultipleRegistersRequest request = ModbusRequestBuilder.buildWriteMultipleRegisters(address.getAddress(), registers);
            try {
                return executeWithClient(client -> {
                    CompletionStage<WriteMultipleRegistersResponse> future = client.writeMultipleRegistersAsync(unitId, request);
                    WriteMultipleRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                    return response != null;
                });
            } finally {
                ReferenceCountUtil.release(request);
            }
        }
    }

    // =============== 鍛戒护鎵ц鏂规硶 ===============

    private Object executeReadMultipleRegisters(int unitId,Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        int quantity = (int) params.getOrDefault("quantity", 1);
        ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(address, quantity);

        try {
            ReadHoldingRegistersResponse response = executeWithClient(client -> {
                CompletionStage<ReadHoldingRegistersResponse> future = client.readHoldingRegistersAsync(unitId, request);
                return future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            });
            List<Short> values = new ArrayList<>();

            if (response != null && response.registers() != null) {
                byte[] raw = response.registers();
                java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(raw);
                buffer.order(java.nio.ByteOrder.BIG_ENDIAN);

                for (int i = 0; i < quantity; i++) {
                    values.add(buffer.getShort());
                }
            }

            return Map.of(
                    "success", response != null,
                    "address", address,
                    "quantity", quantity,
                    "values", values
            );
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    private Object executeWriteMultipleRegisters(int unitId,Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        @SuppressWarnings("unchecked")
        List<Integer> values = (List<Integer>) params.get("values");
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("values鍙傛暟涓嶈兘涓虹┖");
        }

        WriteMultipleRegistersRequest request = ModbusRequestBuilder.buildWriteMultipleRegisters(address, values);

        try {
            WriteMultipleRegistersResponse response = executeWithClient(client -> {
                CompletionStage<WriteMultipleRegistersResponse> future = client.writeMultipleRegistersAsync(unitId, request);
                return future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            });
            return Map.of(
                    "success", response != null,
                    "address", address,
                    "quantity", values.size()
            );
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    private Object executeReadCoils(int unitId,Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        int quantity = (int) params.getOrDefault("quantity", 1);

        ReadCoilsResponse response = executeWithClient(client -> {
            CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(unitId, new ReadCoilsRequest(address, quantity));
            return future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        });
        List<Boolean> values = ModbusUtils.getCoilValues(response.coils(), quantity);

        return Map.of(
                "success", true,
                "address", address,
                "quantity", quantity,
                "values", values
        );
    }

    private Object executeWriteCoils(int unitId,Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        @SuppressWarnings("unchecked")
        List<Boolean> values = (List<Boolean>) params.get("values");
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("values鍙傛暟涓嶈兘涓虹┖");
        }

        byte[] coilBytes = ModbusUtils.buildCoilBytes(values);
        WriteMultipleCoilsResponse response = executeWithClient(client -> client.writeMultipleCoils(
                unitId,
                new WriteMultipleCoilsRequest(address, values.size(), coilBytes)
        ));

        return Map.of(
                "success", response != null,
                "address", address,
                "quantity", values.size()
        );
    }

    private Object executeDiagnostic(int unitId,Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        result.put("protocol", "Modbus TCP");
        result.put("host", host);
        result.put("port", port);
        result.put("unitId", unitId);
        result.put("timeout", timeout);
        result.put("masterConnected", connectionAdapter != null && connectionAdapter.isConnected());
        result.put("timestamp", System.currentTimeMillis());

        try {
            boolean connected = testConnection();
            result.put("deviceConnected", connected);
            result.put("connectionTest", "SUCCESS");
        } catch (Exception e) {
            result.put("deviceConnected", false);
            result.put("connectionTest", "FAILED");
            result.put("error", e.getMessage());
        }

        return result;
    }

    // =============== 杈呭姪鏂规硶 ===============

    private boolean testConnection() {
        try {
            executeWithClient(client -> {
                client.readHoldingRegisters(1, new ReadHoldingRegistersRequest(0, 1));
                return true;
            });
            return true;
        } catch (Exception e) {
            log.warn("杩炴帴娴嬭瘯澶辫触", e);
            return false;
        }
    }

    private List<Integer> collectUnitIds() {
        Set<Integer> unitIds = new LinkedHashSet<>();
        for (ModbusReadPlan plan : readPlans) {
            unitIds.add(plan.getUnitId());
        }
        if (unitIds.isEmpty()) {
            Integer configured = getConfiguredSlaveId();
            if (configured != null) {
                unitIds.add(configured);
            }
        }
        if (unitIds.isEmpty()) {
            unitIds.add(1);
        }
        return new ArrayList<>(unitIds);
    }

    private Integer getConfiguredSlaveId() {
        if (deviceInfo == null || deviceInfo.getProtocolConfig() == null) {
            return null;
        }
        Object value = deviceInfo.getProtocolConfig().get("slaveId");
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String str && !str.isBlank()) {
            try {
                return Integer.parseInt(str);
            } catch (NumberFormatException e) {
                log.warn("鏃犳硶瑙ｆ瀽璁惧 {} 鐨?slaveId: {}", deviceInfo.getDeviceId(), str);
            }
        }
        return null;
    }

    private void processWriteBatch(BatchKey key,
                                   List<WriteEntry> entries,
                                   Map<String, Boolean> results) {
        int limit = key.registerType == RegisterType.COIL ? MAX_WRITE_COILS : MAX_WRITE_REGISTERS;
        List<WriteEntry> chunk = new ArrayList<>();
        int chunkStart = -1;
        int chunkQuantity = 0;

        for (WriteEntry entry : entries) {
            if (chunk.isEmpty()) {
                chunk.add(entry);
                chunkStart = entry.address();
                chunkQuantity = entry.registerCount();
                continue;
            }

            int expectedAddress = chunkStart + chunkQuantity;
            boolean contiguous = entry.address() == expectedAddress;
            boolean exceeds = chunkQuantity + entry.registerCount() > limit;

            if (!contiguous || exceeds) {
                flushWriteChunk(key, chunkStart, chunk, results);
                chunk = new ArrayList<>();
                chunk.add(entry);
                chunkStart = entry.address();
                chunkQuantity = entry.registerCount();
            } else {
                chunk.add(entry);
                chunkQuantity += entry.registerCount();
            }
        }

        if (!chunk.isEmpty()) {
            flushWriteChunk(key, chunkStart, chunk, results);
        }
    }

    private void flushWriteChunk(BatchKey key,
                                 int startAddress,
                                 List<WriteEntry> chunk,
                                 Map<String, Boolean> results) {
        if (chunk.isEmpty()) {
            return;
        }

        if (chunk.size() == 1) {
            writeEntriesIndividually(chunk, results);
            return;
        }

        boolean success = key.registerType == RegisterType.COIL
                ? writeCoilChunk(key.unitId, startAddress, chunk)
                : writeHoldingChunk(key.unitId, startAddress, chunk);

        if (success) {
            chunk.forEach(entry -> results.put(entry.point().getPointId(), true));
        } else {
            writeEntriesIndividually(chunk, results);
        }
    }

    private void writeEntriesIndividually(List<WriteEntry> chunk,
                                          Map<String, Boolean> results) {
        for (WriteEntry entry : chunk) {
            try {
                boolean single = doWritePoint(entry.point(), entry.value());
                results.put(entry.point().getPointId(), single);
            } catch (Exception e) {
                log.error("鍗曠偣鍐欏叆澶辫触: {}", entry.point().getPointName(), e);
                results.put(entry.point().getPointId(), false);
            }
        }
    }

    private boolean writeCoilChunk(int unitId, int startAddress, List<WriteEntry> chunk) {
        try {
            return executeWithClient(client -> {
                List<Boolean> values = new ArrayList<>();
                for (WriteEntry entry : chunk) {
                    values.add(asBoolean(entry.value()));
                }
                byte[] coilBytes = ModbusUtils.buildCoilBytes(values);
                WriteMultipleCoilsResponse response = client.writeMultipleCoils(
                        unitId,
                        new WriteMultipleCoilsRequest(startAddress, values.size(), coilBytes)
                );
                return response != null;
            });
        } catch (Exception e) {
            log.error("鎵归噺鍐欑嚎鍦堝け璐? unitId={}, startAddress={}", unitId, startAddress, e);
            return false;
        }
    }

    private boolean writeHoldingChunk(int unitId, int startAddress, List<WriteEntry> chunk) {
        short[] registers = buildRegisterBuffer(chunk);
        WriteMultipleRegistersRequest request = ModbusRequestBuilder.buildWriteMultipleRegisters(
                startAddress,
                registers
        );
        try {
            return executeWithClient(client -> {
                CompletionStage<WriteMultipleRegistersResponse> future = client.writeMultipleRegistersAsync(unitId, request);
                WriteMultipleRegistersResponse response = future.toCompletableFuture()
                        .get(timeout, TimeUnit.MILLISECONDS);
                return response != null;
            });
        } catch (Exception e) {
            log.error("鎵归噺鍐欎繚鎸佸瘎瀛樺櫒澶辫触: unitId={}, startAddress={}", unitId, startAddress, e);
            return false;
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    private short[] buildRegisterBuffer(List<WriteEntry> chunk) {
        int total = chunk.stream().mapToInt(WriteEntry::registerCount).sum();
        short[] buffer = new short[total];
        int offset = 0;
        for (WriteEntry entry : chunk) {
            short[] values = ModbusUtils.valueToRegisters(
                    entry.value(),
                    entry.point().getDataType(),
                    java.nio.ByteOrder.BIG_ENDIAN
            );
            System.arraycopy(values, 0, buffer, offset, values.length);
            offset += values.length;
        }
        return buffer;
    }

    private <T> T executeWithClient(ModbusTcpConnectionAdapter.ModbusCallable<T> callable) throws Exception {
        return requireConnection().execute(callable, timeout);
    }

    private ModbusTcpConnectionAdapter requireConnection() {
        if (connectionAdapter == null) {
            throw new IllegalStateException("Modbus TCP连接尚未建立");
        }
        return connectionAdapter;
    }

    private ConnectionConfig buildConnectionConfig() {
        ConnectionConfig config = new ConnectionConfig();
        config.setDeviceId(deviceInfo.getDeviceId());
        config.setDeviceName(deviceInfo.getDeviceName());
        config.setProtocolType(getProtocolType());
        config.setConnectionType("MODBUS_TCP");
        config.setGroupId(deviceInfo.getGroupId());
        CollectorProperties.ModbusConfig defaults = collectorProperties != null
                ? collectorProperties.getModbus()
                : new CollectorProperties.ModbusConfig();
        Map<String, Object> protocol = deviceInfo.getProtocolConfig() != null
                ? new HashMap<>(deviceInfo.getProtocolConfig())
                : new HashMap<>();
        host = deviceInfo.getIpAddress() != null ? deviceInfo.getIpAddress() : (String) protocol.getOrDefault("host", "127.0.0.1");
        Object portOverride = protocol.getOrDefault("port", deviceInfo.getPort());
        port = portOverride instanceof Number ? ((Number) portOverride).intValue()
                : portOverride != null ? Integer.parseInt(portOverride.toString()) : 502;
        int timeoutConfig = resolveInt(protocol, "timeout", defaults.getTimeout());
        config.setHost(host);
        config.setPort(port);
        config.setReadTimeout(timeoutConfig);
        config.setWriteTimeout(timeoutConfig);
        config.setConnectTimeout(resolveInt(deviceInfo.getConnectionConfig(), "connectTimeout", 5000));
        config.setMaxGroupConnections(resolveInt(deviceInfo.getConnectionConfig(), "maxGroupConnections", 0));
        config.setMaxPendingMessages(resolveInt(deviceInfo.getConnectionConfig(), "maxPendingMessages", 1024));
        config.setDispatchBatchSize(resolveInt(deviceInfo.getConnectionConfig(), "dispatchBatchSize", 1));
        config.setDispatchFlushInterval(resolveLong(deviceInfo.getConnectionConfig(), "dispatchFlushInterval", 0L));
        config.setOverflowStrategy(resolveString(deviceInfo.getConnectionConfig(), "overflowStrategy", "BLOCK"));
        config.setProtocolConfig(protocol);
        if (deviceInfo.getConnectionConfig() != null) {
            config.setExtraParams(new HashMap<>(deviceInfo.getConnectionConfig()));
        }
        return config;
    }

    private int resolveInt(Map<String, Object> source, String key, int defaultValue) {
        if (source == null || !source.containsKey(key) || source.get(key) == null) {
            return defaultValue;
        }
        Object value = source.get(key);
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private long resolveLong(Map<String, Object> source, String key, long defaultValue) {
        if (source == null || !source.containsKey(key) || source.get(key) == null) {
            return defaultValue;
        }
        Object value = source.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private String resolveString(Map<String, Object> source, String key, String defaultValue) {
        if (source == null || !source.containsKey(key) || source.get(key) == null) {
            return defaultValue;
        }
        String val = source.get(key).toString();
        return val.isBlank() ? defaultValue : val;
    }

    private boolean asBoolean(Object value) {
        if (value instanceof Boolean b) {
            return b;
        }
        if (value instanceof Number number) {
            return number.intValue() != 0;
        }
        if (value instanceof String str) {
            return Boolean.parseBoolean(str);
        }
        throw new IllegalArgumentException("鏃犳硶杞崲涓哄竷灏斿€? " + value);
    }

    private record BatchKey(int unitId, RegisterType registerType) {
    }

    private record WriteEntry(DataPoint point,
                              Object value,
                              int address,
                              int registerCount) {
    }

    @Override
    public boolean isConnected() {
        return connectionAdapter != null && connectionAdapter.isConnected();
    }
}


