package com.wangbin.collector.core.collector.protocol.modbus;

import com.digitalpetri.modbus.client.ModbusTcpClient;
import com.digitalpetri.modbus.pdu.*;
import com.digitalpetri.modbus.tcp.client.NettyTcpClientTransport;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.modbus.base.AbstractModbusCollector;
import com.wangbin.collector.core.collector.protocol.modbus.domain.GroupedPoint;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusAddress;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusRequestBuilder;
import com.wangbin.collector.core.collector.protocol.modbus.domain.RegisterType;
import com.wangbin.collector.core.collector.protocol.modbus.utils.ModbusConverter;
import com.wangbin.collector.core.collector.protocol.modbus.utils.ModbusUtils;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Modbus TCP采集器（简化版）
 */
@Slf4j
@Component
public class ModbusTcpCollector extends AbstractModbusCollector {

    private ModbusTcpClient client;
    private String host;
    private int port;

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
        log.info("开始建立Modbus TCP连接: {}", deviceInfo.getDeviceId());
        Map<String, Object> protocolConfig = deviceInfo.getProtocolConfig();
        host = deviceInfo.getIpAddress();
        port = deviceInfo.getPort() != null ? deviceInfo.getPort() : 502;
        slaveId = (Integer) protocolConfig.getOrDefault("slaveId", 1);
        timeout = (Integer) protocolConfig.getOrDefault("timeout", 3000);

        var transport = NettyTcpClientTransport.create(cfg -> {
            cfg.setHostname(host);
            cfg.setPort(port);
        });

        client = ModbusTcpClient.create(transport);
        client.connect();
        log.info("Modbus TCP连接建立成功: {}:{} (SlaveID: {})", host, port, slaveId);
    }

    @Override
    protected void doDisconnect() throws Exception {
        if (client != null) {
            client.disconnect();
            client = null;
        }
        registerCache.clear();
        log.info("Modbus TCP连接已断开");
    }

    @Override
    protected Object doReadPoint(DataPoint point) throws Exception {
        String address = point.getAddress();
        if (address == null || address.isEmpty()) {
            throw new IllegalArgumentException("点位地址不能为空");
        }

        ModbusAddress modbusAddress = parseModbusAddress(address);

        return switch (modbusAddress.getRegisterType()) {
            case COIL -> readCoil(modbusAddress);
            case DISCRETE_INPUT -> readDiscreteInput(modbusAddress);
            case HOLDING_REGISTER -> readHoldingRegister(modbusAddress, point.getDataType());
            case INPUT_REGISTER -> readInputRegister(modbusAddress, point.getDataType());
            default -> throw new IllegalArgumentException("不支持的寄存器类型: " + modbusAddress.getRegisterType());
        };
    }

    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) {
        Map<String, Object> results = new HashMap<>();

        // 按寄存器类型分组
        Map<RegisterType, List<GroupedPoint>> groups = new EnumMap<>(RegisterType.class);

        for (DataPoint point : points) {
            try {
                ModbusAddress address = parseModbusAddress(point.getAddress());
                RegisterType type = address.getRegisterType();
                groups.computeIfAbsent(type, k -> new ArrayList<>())
                        .add(new GroupedPoint(address, point));
            } catch (Exception e) {
                results.put(point.getPointId(), null);
                log.error("解析点位地址失败: {}", point.getAddress(), e);
            }
        }

        // 批量读取每种寄存器类型
        for (Map.Entry<RegisterType, List<GroupedPoint>> entry : groups.entrySet()) {
            RegisterType type = entry.getKey();
            List<GroupedPoint> groupedPoints = entry.getValue();

            // 按连续地址分组
            List<List<GroupedPoint>> pointGroups = groupContinuousPoints(groupedPoints);

            for (List<GroupedPoint> pointGroup : pointGroups) {
                try {
                    Map<String, Object> groupResults = batchReadGroup(type, pointGroup);
                    results.putAll(groupResults);
                } catch (Exception e) {
                    log.error("批量读取失败: {}, 组大小: {}", type, pointGroup.size(), e);
                    for (GroupedPoint gp : pointGroup) {
                        results.put(gp.getPoint().getPointId(), null);
                    }
                }
            }
        }

        return results;
    }

    @Override
    protected boolean doWritePoint(DataPoint point, Object value) throws Exception {
        String address = point.getAddress();
        if (address == null || address.isEmpty()) {
            throw new IllegalArgumentException("点位地址不能为空");
        }

        ModbusAddress modbusAddress = parseModbusAddress(address);

        return switch (modbusAddress.getRegisterType()) {
            case COIL -> writeCoil(modbusAddress, (Boolean) value);
            case HOLDING_REGISTER -> writeHoldingRegister(modbusAddress, value, point.getDataType());
            default -> throw new IllegalArgumentException("该寄存器类型不支持写入: " + modbusAddress.getRegisterType());
        };
    }

    @Override
    protected Map<String, Object> doGetDeviceStatus() {
        Map<String, Object> status = getBaseDeviceStatus("Modbus TCP");
        status.put("host", host);
        status.put("port", port);
        status.put("unitId", slaveId);

        // 测试连接
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
    protected Object doExecuteCommand(String command, Map<String, Object> params) throws Exception {
        return switch (command.toUpperCase()) {
            case "READ_MULTIPLE_REGISTERS" -> executeReadMultipleRegisters(params);
            case "WRITE_MULTIPLE_REGISTERS" -> executeWriteMultipleRegisters(params);
            case "READ_COILS" -> executeReadCoils(params);
            case "WRITE_COILS" -> executeWriteCoils(params);
            case "DIAGNOSTIC" -> executeDiagnostic(params);
            default -> throw new IllegalArgumentException("不支持的Modbus命令: " + command);
        };
    }

    // =============== Modbus操作实现 ===============

    private Boolean readCoil(ModbusAddress address) throws Exception {
        CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(slaveId,
                new ReadCoilsRequest(address.getAddress(), 1));
        ReadCoilsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.parseCoilValue(response.coils(), 0);
    }

    private Boolean readDiscreteInput(ModbusAddress address) throws Exception {
        CompletionStage<ReadDiscreteInputsResponse> future = client.readDiscreteInputsAsync(slaveId,
                new ReadDiscreteInputsRequest(address.getAddress(), 1));
        ReadDiscreteInputsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.parseCoilValue(response.inputs(), 0);
    }

    private Object readHoldingRegister(ModbusAddress address, String dataType) throws Exception {
        int registerCount = ModbusConverter.getRegisterCount(dataType);
        CompletionStage<ReadHoldingRegistersResponse> future = client.readHoldingRegistersAsync(
                slaveId,
                new ReadHoldingRegistersRequest(address.getAddress(), registerCount)
        );
        ReadHoldingRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.parseRegisterValue(response.registers(), dataType);
    }

    private Object readInputRegister(ModbusAddress address, String dataType) throws Exception {
        int registerCount = ModbusConverter.getRegisterCount(dataType);
        CompletionStage<ReadInputRegistersResponse> future = client.readInputRegistersAsync(
                slaveId,
                new ReadInputRegistersRequest(address.getAddress(), registerCount)
        );
        ReadInputRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.parseRegisterValue(response.registers(), dataType);
    }

    private boolean writeCoil(ModbusAddress address, boolean value) throws Exception {
        CompletionStage<WriteSingleCoilResponse> future = client.writeSingleCoilAsync(
                slaveId,
                new WriteSingleCoilRequest(address.getAddress(), value)
        );
        WriteSingleCoilResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return response != null;
    }

    private boolean writeHoldingRegister(ModbusAddress address, Object value, String dataType) throws Exception {
        int registerCount = ModbusConverter.getRegisterCount(dataType);
        short[] registers = ModbusConverter.valueToRegisters(value, dataType, registerCount, java.nio.ByteOrder.BIG_ENDIAN);

        if (registerCount == 1) {
            WriteSingleRegisterRequest request = new WriteSingleRegisterRequest(address.getAddress(), registers[0]);
            CompletionStage<WriteSingleRegisterResponse> future = client.writeSingleRegisterAsync(slaveId, request);

            try {
                WriteSingleRegisterResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                return response != null;
            } finally {
                ReferenceCountUtil.release(request);
            }
        } else {
            WriteMultipleRegistersRequest request = ModbusRequestBuilder.buildWriteMultipleRegisters(address.getAddress(), registers);
            CompletionStage<WriteMultipleRegistersResponse> future = client.writeMultipleRegistersAsync(slaveId, request);
            try {
                WriteMultipleRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                return response != null;
            } finally {
                ReferenceCountUtil.release(request);
            }
        }
    }

    private Map<String, Object> batchReadGroup(RegisterType type, List<GroupedPoint> pointGroup) throws Exception {
        if (pointGroup.isEmpty()) {
            return Collections.emptyMap();
        }

        int[] range = com.wangbin.collector.core.collector.protocol.modbus.utils.ModbusGroupingUtil.getAddressRange(pointGroup);
        int minAddress = range[0];
        int maxAddress = range[1];
        int quantity = maxAddress - minAddress + 1;

        return switch (type) {
            case COIL -> batchReadCoils(pointGroup, minAddress, quantity);
            case DISCRETE_INPUT -> batchReadDiscreteInputs(pointGroup, minAddress, quantity);
            case HOLDING_REGISTER -> batchReadHoldingRegisters(pointGroup, minAddress, quantity);
            case INPUT_REGISTER -> batchReadInputRegisters(pointGroup, minAddress, quantity);
            default -> throw new IllegalArgumentException("不支持的批量读取类型: " + type);
        };
    }

    private Map<String, Object> batchReadCoils(List<GroupedPoint> pointGroup, int minAddress, int quantity) throws Exception {
        ReadCoilsRequest request = new ReadCoilsRequest(minAddress, quantity);
        CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(slaveId, request);

        try {
            ReadCoilsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            Map<String, Object> results = new HashMap<>();
            List<Boolean> bools = ModbusUtils.getCoilValues(response.coils(), quantity);

            for (GroupedPoint gp : pointGroup) {
                int offset = gp.getAddress().getAddress() - minAddress;
                if (offset >= 0 && offset < bools.size()) {
                    results.put(gp.getPoint().getPointId(), bools.get(offset));
                }
            }

            return results;
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    private Map<String, Object> batchReadDiscreteInputs(List<GroupedPoint> pointGroup, int minAddress, int quantity) throws Exception {
        ReadDiscreteInputsRequest request = new ReadDiscreteInputsRequest(minAddress, quantity);
        CompletionStage<ReadDiscreteInputsResponse> future = client.readDiscreteInputsAsync(slaveId, request);

        try {
            ReadDiscreteInputsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            Map<String, Object> results = new HashMap<>();

            if (response != null && response.inputs() != null) {
                byte[] raw = response.inputs();

                for (GroupedPoint gp : pointGroup) {
                    int offset = gp.getAddress().getAddress() - minAddress;
                    int byteIndex = offset / 8;
                    int bitIndex = offset % 8;

                    if (byteIndex < raw.length) {
                        boolean value = ((raw[byteIndex] >> bitIndex) & 0x01) == 1;
                        results.put(gp.getPoint().getPointId(), value);
                    }
                }
            }

            return results;
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    private Map<String, Object> batchReadHoldingRegisters(List<GroupedPoint> pointGroup, int minAddress, int quantity) throws Exception {
        int maxRegisters = pointGroup.stream()
                .mapToInt(gp -> ModbusConverter.getRegisterCount(gp.getPoint().getDataType()))
                .max()
                .orElse(1);

        ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(minAddress, maxRegisters);
        CompletionStage<ReadHoldingRegistersResponse> future = client.readHoldingRegistersAsync(slaveId, request);

        try {
            ReadHoldingRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            Map<String, Object> results = new HashMap<>();

            if (response != null && response.registers() != null) {
                byte[] raw = response.registers();

                for (GroupedPoint gp : pointGroup) {
                    int offsetRegister = gp.getAddress().getAddress() - minAddress;
                    String dataType = gp.getPoint().getDataType();
                    Object value = ModbusUtils.parseValue(raw, offsetRegister, dataType);
                    results.put(gp.getPoint().getPointId(), value);
                }
            }

            return results;
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    private Map<String, Object> batchReadInputRegisters(List<GroupedPoint> pointGroup, int minAddress, int quantity) throws Exception {
        int maxRegisters = pointGroup.stream()
                .mapToInt(gp -> ModbusConverter.getRegisterCount(gp.getPoint().getDataType()))
                .max()
                .orElse(1);

        ReadInputRegistersRequest request = new ReadInputRegistersRequest(minAddress, maxRegisters);
        CompletionStage<ReadInputRegistersResponse> future = client.readInputRegistersAsync(slaveId, request);

        try {
            ReadInputRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            Map<String, Object> results = new HashMap<>();

            if (response != null && response.registers() != null) {
                byte[] raw = response.registers();

                for (GroupedPoint gp : pointGroup) {
                    int offsetRegister = gp.getAddress().getAddress() - minAddress;
                    String dataType = gp.getPoint().getDataType();
                    Object value = ModbusUtils.parseValue(raw, offsetRegister, dataType);
                    results.put(gp.getPoint().getPointId(), value);
                }
            }

            return results;
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    // =============== 命令执行方法 ===============

    private Object executeReadMultipleRegisters(Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        int quantity = (int) params.getOrDefault("quantity", 1);

        ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(address, quantity);
        CompletionStage<ReadHoldingRegistersResponse> future = client.readHoldingRegistersAsync(slaveId, request);

        try {
            ReadHoldingRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
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

    private Object executeWriteMultipleRegisters(Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        @SuppressWarnings("unchecked")
        List<Integer> values = (List<Integer>) params.get("values");
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("values参数不能为空");
        }

        WriteMultipleRegistersRequest request = ModbusRequestBuilder.buildWriteMultipleRegisters(address, values);
        CompletionStage<WriteMultipleRegistersResponse> future = client.writeMultipleRegistersAsync(slaveId, request);

        try {
            WriteMultipleRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            return Map.of(
                    "success", response != null,
                    "address", address,
                    "quantity", values.size()
            );
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    private Object executeReadCoils(Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        int quantity = (int) params.getOrDefault("quantity", 1);

        CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(slaveId, new ReadCoilsRequest(address, quantity));
        ReadCoilsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        List<Boolean> values = ModbusUtils.getCoilValues(response.coils(), quantity);

        return Map.of(
                "success", true,
                "address", address,
                "quantity", quantity,
                "values", values
        );
    }

    private Object executeWriteCoils(Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        @SuppressWarnings("unchecked")
        List<Boolean> values = (List<Boolean>) params.get("values");
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("values参数不能为空");
        }

        byte[] coilBytes = ModbusUtils.buildCoilBytes(values);
        WriteMultipleCoilsResponse response = client.writeMultipleCoils(
                slaveId,
                new WriteMultipleCoilsRequest(address, values.size(), coilBytes)
        );

        return Map.of(
                "success", response != null,
                "address", address,
                "quantity", values.size()
        );
    }

    private Object executeDiagnostic(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        result.put("protocol", "Modbus TCP");
        result.put("host", host);
        result.put("port", port);
        result.put("unitId", slaveId);
        result.put("timeout", timeout);
        result.put("masterConnected", client != null);
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

    // =============== 辅助方法 ===============

    private boolean testConnection() {
        try {
            client.readHoldingRegisters(slaveId, new ReadHoldingRegistersRequest(0, 1));
            return true;
        } catch (Exception e) {
            log.warn("连接测试失败", e);
            return false;
        }
    }

    @Override
    public boolean isConnected() {
        return client != null;
    }
}