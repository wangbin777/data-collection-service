package com.wangbin.collector.core.collector.protocol.modbus;

import com.digitalpetri.modbus.client.ModbusTcpClient;
import com.digitalpetri.modbus.pdu.*;
import com.digitalpetri.modbus.tcp.client.NettyTcpClientTransport;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.enums.DataType;
import com.wangbin.collector.core.collector.protocol.modbus.base.AbstractModbusCollector;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusAddress;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusRequestBuilder;
import com.wangbin.collector.core.collector.protocol.modbus.plan.ModbusReadPlan;
import com.wangbin.collector.core.collector.protocol.modbus.plan.PointOffset;
import com.wangbin.collector.core.collector.protocol.modbus.utils.ModbusUtils;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
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
    private final Semaphore tcpSemaphore = new Semaphore(1);
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
        timeout = (Integer) protocolConfig.getOrDefault("timeout", 3000);

        var transport = NettyTcpClientTransport.create(cfg -> {
            cfg.setHostname(host);
            cfg.setPort(port);
        });

        client = ModbusTcpClient.create(transport);
        client.connect();
        log.info("Modbus TCP连接建立成功: {}:{} ", host, port);

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
        int unitId = resolveUnitId(point);
        return switch (modbusAddress.getRegisterType()) {
            case COIL -> readCoil(unitId,modbusAddress);
            case DISCRETE_INPUT -> readDiscreteInput(unitId,modbusAddress);
            case HOLDING_REGISTER -> readHoldingRegister(unitId,modbusAddress, point.getDataType());
            case INPUT_REGISTER -> readInputRegister(unitId,modbusAddress, point.getDataType());
            default -> throw new IllegalArgumentException("不支持的寄存器类型: " + modbusAddress.getRegisterType());
        };
    }

    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) {

        Map<String, Object> results = new HashMap<>();

        for (ModbusReadPlan plan : readPlans) {
            try {
                byte[] raw = executeReadPlan(plan);

                for (PointOffset po : plan.getPoints()) {
                    Object value = ModbusUtils.parseValue(
                            raw,
                            po.getOffset(),
                            DataType.valueOf(po.getDataType())
                    );
                    results.put(po.getPointId(), value);
                }

            } catch (Exception e) {
                log.error("ReadPlan 执行失败: unitId={}, type={}, addr={}",
                        plan.getUnitId(),
                        plan.getRegisterType(),
                        plan.getStartAddress(),
                        e
                );

                for (PointOffset po : plan.getPoints()) {
                    results.put(po.getPointId(), null);
                }
            }
        }

        return results;
    }

    /**
     * 执行计划
     * @param plan
     * @return
     * @throws Exception
     */
    private byte[] executeReadPlan(ModbusReadPlan plan) throws Exception {

        tcpSemaphore.acquire();
        try {

            int timeoutMs = 500 + plan.getQuantity() * 20;

            CompletableFuture<?> future = switch (plan.getRegisterType()) {
                case COIL -> client.readCoilsAsync(plan.getUnitId(),
                        new ReadCoilsRequest(plan.getStartAddress(), plan.getQuantity())
                ).toCompletableFuture();

                case DISCRETE_INPUT ->  client.readDiscreteInputsAsync(
                        plan.getUnitId(),
                        new ReadDiscreteInputsRequest(plan.getStartAddress(), plan.getQuantity())
                ).toCompletableFuture();

                case HOLDING_REGISTER -> client.readHoldingRegistersAsync(plan.getUnitId(),
                        new ReadHoldingRegistersRequest(plan.getStartAddress(), plan.getQuantity())
                ).toCompletableFuture();

                case INPUT_REGISTER -> client.readInputRegistersAsync(plan.getUnitId(),
                        new ReadInputRegistersRequest(plan.getStartAddress(), plan.getQuantity())
                ).toCompletableFuture();

            };

            Object resp = future.get(timeoutMs, TimeUnit.MILLISECONDS);

            return extractRaw(resp);

        } finally {
            tcpSemaphore.release();
        }
    }

    /**
     * 返回结果
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
            throw new IllegalArgumentException("点位地址不能为空");
        }

        ModbusAddress modbusAddress = parseModbusAddress(address);
        int unitId = resolveUnitId(point);
        return switch (modbusAddress.getRegisterType()) {
            case COIL -> writeCoil(unitId,modbusAddress, (Boolean) value);
            case HOLDING_REGISTER -> writeHoldingRegister(unitId,modbusAddress, value, point.getDataType());
            default -> throw new IllegalArgumentException("该寄存器类型不支持写入: " + modbusAddress.getRegisterType());
        };
    }

    @Override
    protected Map<String, Object> doGetDeviceStatus() {
        Map<String, Object> status = getBaseDeviceStatus("Modbus TCP");
        status.put("host", host);
        status.put("port", port);
        status.put("unitId", "1");

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
    protected Object doExecuteCommand(int unitId,String command, Map<String, Object> params) throws Exception {
        return switch (command.toUpperCase()) {
            case "READ_MULTIPLE_REGISTERS" -> executeReadMultipleRegisters(unitId,params);
            case "WRITE_MULTIPLE_REGISTERS" -> executeWriteMultipleRegisters(unitId,params);
            case "READ_COILS" -> executeReadCoils(unitId,params);
            case "WRITE_COILS" -> executeWriteCoils(unitId,params);
            case "DIAGNOSTIC" -> executeDiagnostic(unitId,params);
            default -> throw new IllegalArgumentException("不支持的Modbus命令: " + command);
        };
    }

    // =============== Modbus操作实现 ===============

    private Boolean readCoil(int unitId,ModbusAddress address) throws Exception {
        CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(unitId,
                new ReadCoilsRequest(address.getAddress(), 1));
        ReadCoilsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.parseCoilValue(response.coils(), 0);
    }

    private Boolean readDiscreteInput(int unitId,ModbusAddress address) throws Exception {
        CompletionStage<ReadDiscreteInputsResponse> future = client.readDiscreteInputsAsync(unitId,
                new ReadDiscreteInputsRequest(address.getAddress(), 1));
        ReadDiscreteInputsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.parseCoilValue(response.inputs(), 0);
    }

    private Object readHoldingRegister(int unitId,ModbusAddress address, String dataType) throws Exception {
        int registerCount = DataType.fromString(dataType).getRegisterCount();
        CompletionStage<ReadHoldingRegistersResponse> future = client.readHoldingRegistersAsync(
                unitId,
                new ReadHoldingRegistersRequest(address.getAddress(), registerCount)
        );
        ReadHoldingRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.parseRegisterValue(response.registers(), dataType);
    }

    private Object readInputRegister(int unitId,ModbusAddress address, String dataType) throws Exception {
        int registerCount = DataType.fromString(dataType).getRegisterCount();
        CompletionStage<ReadInputRegistersResponse> future = client.readInputRegistersAsync(
                unitId,
                new ReadInputRegistersRequest(address.getAddress(), registerCount)
        );
        ReadInputRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.parseRegisterValue(response.registers(), dataType);
    }

    private boolean writeCoil(int unitId,ModbusAddress address, boolean value) throws Exception {
        CompletionStage<WriteSingleCoilResponse> future = client.writeSingleCoilAsync(
                unitId,
                new WriteSingleCoilRequest(address.getAddress(), value)
        );
        WriteSingleCoilResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return response != null;
    }

    private boolean writeHoldingRegister(int unitId,ModbusAddress address, Object value, String dataType) throws Exception {
        int registerCount = DataType.fromString(dataType).getRegisterCount();
        short[] registers = ModbusUtils.valueToRegisters(value, dataType, java.nio.ByteOrder.BIG_ENDIAN);

        if (registerCount == 1) {
            WriteSingleRegisterRequest request = new WriteSingleRegisterRequest(address.getAddress(), registers[0]);
            CompletionStage<WriteSingleRegisterResponse> future = client.writeSingleRegisterAsync(unitId, request);

            try {
                WriteSingleRegisterResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                return response != null;
            } finally {
                ReferenceCountUtil.release(request);
            }
        } else {
            WriteMultipleRegistersRequest request = ModbusRequestBuilder.buildWriteMultipleRegisters(address.getAddress(), registers);
            CompletionStage<WriteMultipleRegistersResponse> future = client.writeMultipleRegistersAsync(unitId, request);
            try {
                WriteMultipleRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                return response != null;
            } finally {
                ReferenceCountUtil.release(request);
            }
        }
    }

    // =============== 命令执行方法 ===============

    private Object executeReadMultipleRegisters(int unitId,Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        int quantity = (int) params.getOrDefault("quantity", 1);
        ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(address, quantity);
        CompletionStage<ReadHoldingRegistersResponse> future = client.readHoldingRegistersAsync(unitId, request);

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

    private Object executeWriteMultipleRegisters(int unitId,Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        @SuppressWarnings("unchecked")
        List<Integer> values = (List<Integer>) params.get("values");
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("values参数不能为空");
        }

        WriteMultipleRegistersRequest request = ModbusRequestBuilder.buildWriteMultipleRegisters(address, values);
        CompletionStage<WriteMultipleRegistersResponse> future = client.writeMultipleRegistersAsync(unitId, request);

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

    private Object executeReadCoils(int unitId,Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        int quantity = (int) params.getOrDefault("quantity", 1);

        CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(unitId, new ReadCoilsRequest(address, quantity));
        ReadCoilsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
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
            throw new IllegalArgumentException("values参数不能为空");
        }

        byte[] coilBytes = ModbusUtils.buildCoilBytes(values);
        WriteMultipleCoilsResponse response = client.writeMultipleCoils(
                unitId,
                new WriteMultipleCoilsRequest(address, values.size(), coilBytes)
        );

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
            client.readHoldingRegisters(1, new ReadHoldingRegistersRequest(0, 1));
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