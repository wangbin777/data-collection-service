package com.wangbin.collector.core.collector.protocol.modbus;

import com.digitalpetri.modbus.client.ModbusTcpClient;
import com.digitalpetri.modbus.pdu.*;
import com.digitalpetri.modbus.tcp.client.NettyTcpClientTransport;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.enums.DataType;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.collector.protocol.modbus.domain.GroupedPoint;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusAddress;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusUtils;
import com.wangbin.collector.core.collector.protocol.modbus.domain.RegisterType;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Modbus TCP采集器（使用modbus-master-tcp库）
 */
@Slf4j
@Component
public class ModbusTcpCollector extends BaseCollector {

    private ModbusTcpClient client;
    private String host;
    private int port;
    private int unitId;
    private int timeout;
    private final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

    private final Map<RegisterType, Map<Integer, DataPoint>> registerCache = new ConcurrentHashMap<>();

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
        unitId = (Integer) protocolConfig.getOrDefault("slaveId",1);
        timeout = (Integer) protocolConfig.getOrDefault("timeout",3000);

        // Netty transport 默认 timeout
        var transport = NettyTcpClientTransport.create(cfg -> {
            cfg.setHostname(host);
            cfg.setPort(port);
        });

        client = ModbusTcpClient.create(transport);
        client.connect();
        log.info("Modbus TCP连接建立成功: {}:{} (UnitID: {})", host, port, unitId);
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

        // 解析Modbus地址
        ModbusAddress modbusAddress = parseModbusAddress(address);

        // 根据寄存器类型读取数据
        return switch (modbusAddress.getRegisterType()) {
            case COIL -> readCoil(modbusAddress);
            case DISCRETE_INPUT -> readDiscreteInput(modbusAddress);
            case HOLDING_REGISTER -> readHoldingRegister(modbusAddress, point.getDataType());
            case INPUT_REGISTER -> readInputRegister(modbusAddress, point.getDataType());
            default -> throw new IllegalArgumentException("不支持的Modbus寄存器类型: " +
                    modbusAddress.getRegisterType());
        };
    }

    /*@Override
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
                    // 标记该组读取失败
                    for (GroupedPoint gp : pointGroup) {
                        results.put(gp.point.getPointId(), null);
                    }
                }
            }
        }

        return results;
    }*/

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
            default -> throw new IllegalArgumentException("该寄存器类型不支持写入: " +
                    modbusAddress.getRegisterType());
        };
    }

    @Override
    protected Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) throws Exception {
        Map<String, Boolean> results = new HashMap<>();

        // 由于Modbus批量写入限制，逐个写入
        for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
            DataPoint point = entry.getKey();
            Object value = entry.getValue();

            try {
                boolean success = writePoint(point, value);
                results.put(point.getPointId(), success);
            } catch (Exception e) {
                results.put(point.getPointId(), false);
                log.error("写入点位失败: {}", point.getPointName(), e);
            }
        }

        return results;
    }

    @Override
    protected void doSubscribe(List<DataPoint> points) {
        log.info("Modbus TCP订阅: 数量={}", points.size());

        // Modbus不支持原生订阅，这里记录订阅关系用于轮询
        for (DataPoint point : points) {
            try {
                ModbusAddress address = parseModbusAddress(point.getAddress());
                RegisterType type = address.getRegisterType();

                registerCache.computeIfAbsent(type, k -> new ConcurrentHashMap<>())
                        .put(address.getAddress(), point);
            } catch (Exception e) {
                log.error("订阅点位失败: {}", point.getAddress(), e);
            }
        }
    }

    @Override
    protected void doUnsubscribe(List<DataPoint> points) {
        log.info("取消Modbus TCP订阅: 数量={}", points.size());

        if (points.isEmpty()) {
            registerCache.clear();
        } else {
            for (DataPoint point : points) {
                try {
                    ModbusAddress address = parseModbusAddress(point.getAddress());
                    RegisterType type = address.getRegisterType();

                    Map<Integer, DataPoint> typeCache = registerCache.get(type);
                    if (typeCache != null) {
                        typeCache.remove(address.getAddress());

                        if (typeCache.isEmpty()) {
                            registerCache.remove(type);
                        }
                    }
                } catch (Exception e) {
                    log.error("取消订阅点位失败: {}", point.getAddress(), e);
                }
            }
        }
    }

    @Override
    protected Map<String, Object> doGetDeviceStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("protocol", "Modbus TCP");
        status.put("host", host);
        status.put("port", port);
        status.put("unitId", unitId);
        status.put("timeout", timeout);
        status.put("byteOrder", byteOrder.toString());
        status.put("masterConnected", client != null);

        // 统计订阅信息
        int totalSubscribed = 0;
        Map<String, Integer> subscribedByType = new HashMap<>();
        for (Map.Entry<RegisterType, Map<Integer, DataPoint>> entry : registerCache.entrySet()) {
            int count = entry.getValue().size();
            subscribedByType.put(entry.getKey().name(), count);
            totalSubscribed += count;
        }
        status.put("subscribedPoints", totalSubscribed);
        status.put("subscribedByType", subscribedByType);

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

    /**
    * 读取线圈
    */
    private Boolean readCoil(ModbusAddress address) throws Exception {
        CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(unitId,
                new ReadCoilsRequest(address.getAddress(), 1));
        // 阻塞等待结果
        ReadCoilsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.getCoilValue(response.coils());
    }

    /**
     * 读取离散输入
     * @param address
     * @return
     * @throws Exception
     */
    private Boolean readDiscreteInput(ModbusAddress address) throws Exception {
        CompletionStage<ReadDiscreteInputsResponse> future = client.readDiscreteInputsAsync(unitId,
                new ReadDiscreteInputsRequest(address.getAddress(), 1));
        // 阻塞等待结果
        ReadDiscreteInputsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.getCoilValue(response.inputs());
    }

    /**
     * 读取保持寄存器
     * @param address
     * @param dataType
     * @return
     * @throws Exception
     */
    private Object readHoldingRegister(ModbusAddress address, String dataType) throws Exception {
        CompletionStage<ReadHoldingRegistersResponse> future = client.readHoldingRegistersAsync(
                unitId,
                new ReadHoldingRegistersRequest(address.getAddress(), DataType.fromString(dataType).getRegisterCount())
        );
        ReadHoldingRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.convertByteToValue(response.registers(), dataType);
    }

    /**
     * 读取输入寄存器
     * @param address
     * @param dataType
     * @return
     * @throws Exception
     */

    private Object readInputRegister(ModbusAddress address, String dataType) throws Exception {
        CompletionStage<ReadInputRegistersResponse> future = client.readInputRegistersAsync(
                unitId,
                new ReadInputRegistersRequest(address.getAddress(), DataType.fromString(dataType).getRegisterCount())
        );
        ReadInputRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.convertByteToValue(response.registers(), dataType);
    }

    /**
     * 写入线圈
      */
    private boolean writeCoil(ModbusAddress address, boolean value) throws Exception {
        CompletionStage<WriteSingleCoilResponse> future = client.writeSingleCoilAsync(
                unitId,
                new WriteSingleCoilRequest(address.getAddress(), value)
        );
        WriteSingleCoilResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return response != null;
    }

    /**
     * 写入保持寄存器
     */
    private boolean writeHoldingRegister(ModbusAddress address, Object value, String dataType) throws Exception {
        int registerCount = DataType.fromString(dataType).getRegisterCount();
        short[] registers = convertValueToRegisters(value, dataType, registerCount);

        if (registerCount == 1) {
            // 写入单个寄存器
            WriteSingleRegisterRequest request = new WriteSingleRegisterRequest(address.getAddress(), registers[0]);
            CompletionStage<WriteSingleRegisterResponse> future = client.writeSingleRegisterAsync(unitId,request);

            try {
                WriteSingleRegisterResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                return response != null;
            } catch (Exception e) {
                throw new Exception("写入保持寄存器失败: " + e.getMessage(), e);
            } finally {
                ReferenceCountUtil.release(request);
            }
        } else {
            // 写入多个寄存器 - 使用辅助方法
            WriteMultipleRegistersRequest request = buildWriteMultipleRegisters(
                    address.getAddress(), registers);
            CompletionStage<WriteMultipleRegistersResponse> future = client.writeMultipleRegistersAsync(unitId,request);
            try {
                WriteMultipleRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                return response != null;
            } catch (Exception e) {
                throw new Exception("写入多个保持寄存器失败: " + e.getMessage(), e);
            } finally {
                ReferenceCountUtil.release(request);
            }
        }
    }

    /**
     * 批量读取组
     */
    private Map<String, Object> batchReadGroup(RegisterType type, List<GroupedPoint> pointGroup) throws Exception {
        if (pointGroup.isEmpty()) {
            return Collections.emptyMap();
        }

        // 找到最小和最大地址
        int minAddress = pointGroup.stream()
                .mapToInt(gp -> gp.getAddress().getAddress())
                .min()
                .orElse(0);
        int maxAddress = pointGroup.stream()
                .mapToInt(gp -> gp.getAddress().getAddress())
                .max()
                .orElse(0);

        int quantity = maxAddress - minAddress + 1;

        return switch (type) {
            case COIL -> batchReadCoils(pointGroup, minAddress, quantity);
            case DISCRETE_INPUT -> batchReadDiscreteInputs(pointGroup, minAddress, quantity);
            case HOLDING_REGISTER -> batchReadHoldingRegisters(pointGroup, minAddress, quantity);
            case INPUT_REGISTER -> batchReadInputRegisters(pointGroup, minAddress, quantity);
            default -> throw new IllegalArgumentException("不支持的批量读取类型: " + type);
        };
    }

    /**
     * 批量读取线圈
     * @param pointGroup
     * @param minAddress
     * @param quantity
     * @return
     * @throws Exception
     */
    private Map<String, Object> batchReadCoils(List<GroupedPoint> pointGroup, int minAddress, int quantity) throws Exception {
        CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(unitId,
                new ReadCoilsRequest(minAddress, quantity));
        ReadCoilsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);

        Map<String, Object> results = new HashMap<>();
        List<Boolean> bools = ModbusUtils.getCoilValues(response.coils(),quantity);
        for (GroupedPoint gp : pointGroup) {
            int offset = gp.getAddress().getAddress() - minAddress;
            if (offset >= 0 && offset < bools.size()) {
                results.put(gp.getPoint().getPointId(), bools.get(offset));
            }
        }
        return results;
    }


    /**
     * 批量读取离散输入（稳定、不越界、不触发引用计数错误）
     * @param pointGroup
     * @param minAddress
     * @param quantity
     * @return
     * @throws Exception
     */
    private Map<String, Object> batchReadDiscreteInputs(List<GroupedPoint> pointGroup,int minAddress,
                                                        int quantity) throws Exception {
        if (pointGroup.isEmpty()) return Collections.emptyMap();

        ReadDiscreteInputsRequest request = new ReadDiscreteInputsRequest(minAddress, quantity);
        CompletionStage<ReadDiscreteInputsResponse> future =
                client.readDiscreteInputsAsync(unitId, request);

        try {
            ReadDiscreteInputsResponse response = future.toCompletableFuture()
                    .get(timeout, TimeUnit.MILLISECONDS);

            Map<String, Object> results = new HashMap<>();

            if (response != null && response.inputs() != null) {
                byte[] raw = response.inputs();

                for (GroupedPoint gp : pointGroup) {
                    int offset = gp.getAddress().getAddress() - minAddress;
                    int byteIndex = offset / 8;
                    int bitIndex = offset % 8;

                    if (byteIndex >= raw.length) {
                        log.warn("离散输入字节越界，跳过点 {}", gp.getPoint().getPointId());
                        results.put(gp.getPoint().getPointId(), null);
                        continue;
                    }

                    boolean value = ((raw[byteIndex] >> bitIndex) & 0x01) == 1;
                    results.put(gp.getPoint().getPointId(), value);
                }
            }

            return results;
        } finally {
            ReferenceCountUtil.release(request);
        }
    }
    /*private Map<String, Object> batchReadDiscreteInputs(List<GroupedPoint> pointGroup,
                                                        int minAddress,
                                                        int quantity) throws Exception {

        ReadDiscreteInputsRequest request = new ReadDiscreteInputsRequest(minAddress, quantity);

        CompletionStage<ReadDiscreteInputsResponse> future =
                client.readDiscreteInputsAsync(unitId, request);

        try {
            ReadDiscreteInputsResponse response =
                    future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);

            Map<String, Object> results = new HashMap<>();

            if (response == null || response.inputs() == null) {
                log.warn("response == null 或 inputs() == null");
                return results;
            }

            // ---- 关键：新版直接返回 byte[] ----
            byte[] raw = response.inputs();
            int bytes = raw.length;

            log.info("DiscreteInputs 返回字节: {}, hex: {}",
                    bytes,
                    DatatypeConverter.printHexBinary(raw));

            // ---- 使用工具类解析 ----
            for (GroupedPoint gp : pointGroup) {

                int offset = gp.address.getAddress() - minAddress; // bit 偏移
                if (offset < 0 || offset >= quantity) {
                    continue;
                }
                boolean value = ModbusUtils.parseBit(raw, offset);

                results.put(gp.point.getPointId(), value);
            }

            return results;

        } finally {
            // 只释放 request
            ReferenceCountUtil.release(request);
        }
    }*/




    /**
     * 批量读取保持寄存器
     * @param pointGroup
     * @param minAddress
     * @param quantity
     * @return
     * @throws Exception
     */
    private Map<String, Object> batchReadHoldingRegisters(List<GroupedPoint> pointGroup,
                                                          int minAddress,int quantity) throws Exception {

        // 计算本组总共需要读取多少寄存器（按最大寄存器需求计算）
        int maxRegisters = pointGroup.stream()
                .mapToInt(gp -> DataType.fromString(gp.getPoint().getDataType()).getRegisterCount())
                .max()
                .orElse(1);

        ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(minAddress, maxRegisters);
        CompletionStage<ReadHoldingRegistersResponse> future =
                client.readHoldingRegistersAsync(unitId, request);

        try {
            ReadHoldingRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            Map<String, Object> results = new HashMap<>();

            if (response != null && response.registers() != null) {
                byte[] raw = response.registers();

                for (GroupedPoint gp : pointGroup) {
                    int offsetRegister = gp.getAddress().getAddress() - minAddress;
                    int registersNeeded = DataType.fromString(gp.getPoint().getDataType()).getRegisterCount();

                    // 检查数据长度是否足够
                    if ((offsetRegister + registersNeeded) * 2 > raw.length) {
                        log.warn("寄存器数据长度不足，跳过点 {}", gp.getPoint().getPointId());
                        results.put(gp.getPoint().getPointId(), null);
                        continue;
                    }

                    Object value = ModbusUtils.parseValue(raw, offsetRegister, gp.getPoint().getDataType());
                    results.put(gp.getPoint().getPointId(), value);
                }
            }

            return results;
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    /*private Map<String, Object> batchReadHoldingRegisters(List<GroupedPoint> pointGroup,
                                                          int minAddress,
                                                          int quantity) throws Exception {

        ReadHoldingRegistersRequest request =
                new ReadHoldingRegistersRequest(minAddress, quantity);

        CompletionStage<ReadHoldingRegistersResponse> future =
                client.readHoldingRegistersAsync(unitId, request);

        try {
            ReadHoldingRegistersResponse response =
                    future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);

            Map<String, Object> results = new HashMap<>();

            if (response == null || response.registers() == null) {
                log.warn("response == null 或 registers() == null");
                return results;
            }

            byte[] raw = response.registers();   // 新版直接返回 byte[]
            log.info("HoldingRegisters 原始字节：{}", DatatypeConverter.printHexBinary(raw));

            // 遍历每个点
            for (GroupedPoint gp : pointGroup) {
                int offsetReg = gp.address.getAddress() - minAddress;
                if (offsetReg < 0 || offsetReg >= quantity) continue;
                String dataType = gp.point.getDataType();
                // 使用统一工具类解析
                Object value = ModbusUtils.parseValue(raw, offsetReg, dataType);
                results.put(gp.point.getPointId(), value);
            }
            return results;
        } finally {
            ReferenceCountUtil.release(request);
        }
    }*/



    /**
     * 批量读取输入寄存器
     * @param pointGroup
     * @param minAddress
     * @param quantity
     * @return
     * @throws Exception
     */

    private Map<String, Object> batchReadInputRegisters(List<GroupedPoint> pointGroup,
                                                        int minAddress,
                                                        int quantity) throws Exception {
        if (pointGroup.isEmpty()) return Collections.emptyMap();

        quantity = pointGroup.stream()
                .mapToInt(gp -> DataType.fromString(gp.getPoint().getDataType()).getRegisterCount())
                .max()
                .orElse(1);

        ReadInputRegistersRequest request = new ReadInputRegistersRequest(minAddress, quantity);
        CompletionStage<ReadInputRegistersResponse> future =
                client.readInputRegistersAsync(unitId, request);

        try {
            ReadInputRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            Map<String, Object> results = new HashMap<>();

            if (response != null && response.registers() != null) {
                byte[] raw = response.registers();

                for (GroupedPoint gp : pointGroup) {
                    int offsetRegister = gp.getAddress().getAddress() - minAddress;
                    int registersNeeded = DataType.fromString(gp.getPoint().getDataType()).getRegisterCount();

                    if ((offsetRegister + registersNeeded) * 2 > raw.length) {
                        log.warn("输入寄存器数据长度不足，跳过点 {}", gp.getPoint().getPointId());
                        results.put(gp.getPoint().getPointId(), null);
                        continue;
                    }

                    Object value = ModbusUtils.parseValue(raw, offsetRegister, gp.getPoint().getDataType());
                    results.put(gp.getPoint().getPointId(), value);
                }
            }

            return results;
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    /*private Map<String, Object> batchReadInputRegisters(List<GroupedPoint> pointGroup,
                                                        int minAddress,
                                                        int quantity) throws Exception {
        ReadInputRegistersRequest request = new ReadInputRegistersRequest(minAddress, quantity);
        CompletionStage<ReadInputRegistersResponse> future =
                client.readInputRegistersAsync(unitId, request);
        try {
            ReadInputRegistersResponse response =
                    future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            Map<String, Object> results = new HashMap<>();
            if (response == null || response.registers() == null) {
                log.warn("response == null 或 registers() == null");
                return results;
            }
            byte[] raw = response.registers();   // 新版直接 byte[]
            log.info("InputRegisters 原始字节：{}", DatatypeConverter.printHexBinary(raw));
            // 遍历每个点
            for (GroupedPoint gp : pointGroup) {
                int offsetReg = gp.address.getAddress() - minAddress;
                if (offsetReg < 0 || offsetReg >= quantity) continue;
                String dataType = gp.point.getDataType();
                // 使用统一解析工具类 RegisterParser
                Object value = ModbusUtils.parseValue(raw, offsetReg, dataType);
                results.put(gp.point.getPointId(), value);
            }

            return results;

        } finally {
            ReferenceCountUtil.release(request);
        }
    }*/


    // =============== 命令执行方法 ===============

    private Object executeReadMultipleRegisters(Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        int quantity = (int) params.getOrDefault("quantity", 1);

        ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(address, quantity);
        CompletionStage<ReadHoldingRegistersResponse> future =
                client.readHoldingRegistersAsync(unitId, request);

        try {
            ReadHoldingRegistersResponse response =
                    future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);

            List<Short> values = new ArrayList<>();

            if (response != null) {
                byte[] registers = response.registers();

                // ✔ ByteBuffer来解析 short（每2字节）
                ByteBuffer buffer = ByteBuffer.wrap(registers);
                buffer.order(ByteOrder.BIG_ENDIAN);  // ❗Modbus 默认大端，如需要改小端可设 LITTLE_ENDIAN

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


    /**
     * 批量写寄存器
     * @param params
     * @return
     * @throws Exception
     */
    private Object executeWriteMultipleRegisters(Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        @SuppressWarnings("unchecked")
        List<Integer> values = (List<Integer>) params.get("values");
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("values参数不能为空");
        }

        // 构建 byte[] 存放 short 值 (每个寄存器 2 字节)
        byte[] bytes = new byte[values.size() * 2];
        for (int i = 0; i < values.size(); i++) {
            int v = values.get(i);
            // 大端存储
            bytes[i * 2] = (byte) ((v >> 8) & 0xFF);
            bytes[i * 2 + 1] = (byte) (v & 0xFF);
        }

        WriteMultipleRegistersRequest request =
                new WriteMultipleRegistersRequest(address, values.size(), bytes);

        WriteMultipleRegistersResponse response =
                client.writeMultipleRegisters(unitId, request);

        return Map.of(
                "success", response != null,
                "address", address,
                "quantity", values.size()
        );
    }



    private Object executeReadCoils(Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        int quantity = (int) params.getOrDefault("quantity", 1);
        // 异步请求
        CompletionStage<ReadCoilsResponse> future =
                client.readCoilsAsync(unitId, new ReadCoilsRequest(address, quantity));
        // 阻塞等待结果
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

        int quantity = values.size();
        byte[] coilValues = new byte[(quantity + 7) / 8];
        for (int i = 0; i < quantity; i++) {
            if (values.get(i)) {
                coilValues[i / 8] |= 1 << (i % 8);
            }
        }
        WriteMultipleCoilsResponse response = client.writeMultipleCoils(
                unitId,
                new WriteMultipleCoilsRequest(address, quantity, coilValues)
        );
        return Map.of(
                "success", response != null,
                "address", address,
                "quantity", quantity
        );
    }

    private Object executeDiagnostic(Map<String, Object> params) throws Exception {
        Map<String, Object> result = new HashMap<>();
        result.put("protocol", "Modbus TCP");
        result.put("host", host);
        result.put("port", port);
        result.put("unitId", unitId);
        result.put("timeout", timeout);
        result.put("byteOrder", byteOrder.toString());
        result.put("masterConnected", client != null);
        result.put("timestamp", System.currentTimeMillis());

        // 测试连接
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


    /**
     * 测试连接
     * @return
     * @throws Exception
     */
    private boolean testConnection() {
        try {
            client.readHoldingRegisters(unitId, new ReadHoldingRegistersRequest(0,1));
            return true;
        } catch (Exception e) {
            log.warn("连接测试失败", e);
            return false;
        }
    }

    /**
     * 解析Modbus地址
     * @param addressStr
     * @return
     */
    private ModbusAddress parseModbusAddress(String addressStr) {
        if (addressStr == null || addressStr.isEmpty()) {
            throw new IllegalArgumentException("Modbus地址不能为空");
        }

        try {
            // 支持格式: 40001, 30002, 00001, 10001
            // 或者: 4x0001, 3x0002, 0x0001, 1x0001
            // 或者: 4:0001, 3:0002, 0:0001, 1:0001

            int address;
            RegisterType type;

            if (addressStr.contains("x") || addressStr.contains("X") || addressStr.contains(":")) {
                // 格式: 4x0001 或 4:0001
                String[] parts = addressStr.split("[xX:]");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Modbus地址格式错误: " + addressStr);
                }

                int typeCode = Integer.parseInt(parts[0]);
                address = Integer.parseInt(parts[1]);

                type = RegisterType.fromCode(typeCode);
                if (type == null) {
                    throw new IllegalArgumentException("不支持的Modbus寄存器类型: " + typeCode);
                }
            } else {
                // 格式: 40001
                int fullAddress = Integer.parseInt(addressStr);
                int typeCode = fullAddress / 10000;
                address = fullAddress % 10000 - 1; // Modbus地址从0开始

                type = RegisterType.fromCode(typeCode);
                if (type == null) {
                    throw new IllegalArgumentException("不支持的Modbus寄存器类型: " + typeCode);
                }
            }

            return new ModbusAddress(type, address);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Modbus地址格式错误: " + addressStr, e);
        }
    }
    /**
     * 转换单个寄存器为值
     * @param registerValue
     * @param dataType
     * @return
     */
    private Object convertRegisterToValue(short registerValue, String dataType) {
        switch (dataType.toUpperCase()) {
            case "INT16":
                return (int) registerValue;
            case "UINT16":
                return registerValue & 0xFFFF;
            case "BOOLEAN":
                return registerValue != 0;
            default:
                return (int) registerValue;
        }
    }

    /**
     * 转换值为寄存器数组
     * @param value
     * @param dataType
     * @param registerCount
     * @return
     */
    private short[] convertValueToRegisters(Object value, String dataType, int registerCount) {
        if (value == null) {
            return new short[registerCount];
        }

        if (dataType == null) {
            return new short[]{((Number) value).shortValue()};
        }

        switch (dataType.toUpperCase()) {
            case "INT16":
            case "UINT16":
                return new short[]{((Number) value).shortValue()};
            case "INT32":
                return convertInt32ToRegisters(((Number) value).intValue());
            case "UINT32":
                return convertUInt32ToRegisters(((Number) value).longValue() & 0xFFFFFFFFL);
            case "FLOAT":
                return convertFloatToRegisters(((Number) value).floatValue());
            case "DOUBLE":
                return convertDoubleToRegisters(((Number) value).doubleValue());
            case "BOOLEAN":
                return new short[]{((Boolean) value) ? (short) 1 : (short) 0};
            case "STRING":
                return convertStringToRegisters(value.toString(), registerCount);
            default:
                return new short[]{((Number) value).shortValue()};
        }
    }

    /**
     * 转换寄存器为32位整数
     * @param registers
     * @return
     */
    private int convertRegistersToInt32(ByteBuf registers) {
        if (registers.readableBytes() < 4) {
            throw new IllegalArgumentException("需要至少4个字节来转换INT32");
        }

        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(byteOrder);
        buffer.putShort(registers.getShort(0));
        buffer.putShort(registers.getShort(2));
        buffer.flip();

        return buffer.getInt();
    }

    /**
     * 转换寄存器为无符号32位整数
     * @param registers
     * @return
     */
    private long convertRegistersToUInt32(ByteBuf registers) {
        return convertRegistersToInt32(registers) & 0xFFFFFFFFL;
    }

    /**
     * 转换寄存器为浮点数
     * @param registers
     * @return
     */
    private float convertRegistersToFloat(ByteBuf registers) {
        int intBits = convertRegistersToInt32(registers);
        return Float.intBitsToFloat(intBits);
    }

    /**
     * 转换寄存器为双精度浮点数
     * @param registers
     * @return
     */
    private double convertRegistersToDouble(ByteBuf registers) {
        if (registers.readableBytes() < 8) {
            throw new IllegalArgumentException("需要至少8个字节来转换DOUBLE");
        }

        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(byteOrder);

        for (int i = 0; i < 4; i++) {
            buffer.putShort(registers.getShort(i * 2));
        }
        buffer.flip();

        return buffer.getDouble();
    }

    /**
     * 转换寄存器为字符串
     * @param registers
     * @return
     */
    private String convertRegistersToString(ByteBuf registers) {
        int byteCount = registers.readableBytes();
        byte[] bytes = new byte[byteCount];
        registers.getBytes(0, bytes);

        // 转换为字符串，去除末尾的null字符
        String str = new String(bytes);
        int nullIndex = str.indexOf('\0');
        if (nullIndex != -1) {
            str = str.substring(0, nullIndex);
        }

        return str.trim();
    }

    /**
     * 转换32位整数为寄存器
     * @param value
     * @return
     */
    private short[] convertInt32ToRegisters(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(byteOrder);
        buffer.putInt(value);
        buffer.flip();

        return new short[]{buffer.getShort(), buffer.getShort()};
    }

    /**
     * 转换无符号32位整数为寄存器
     * @param value
     * @return
     */
    private short[] convertUInt32ToRegisters(long value) {
        return convertInt32ToRegisters((int) value);
    }

    /**
     * 转换浮点数为寄存器
     * @param value
     * @return
     */
    private short[] convertFloatToRegisters(float value) {
        int intBits = Float.floatToIntBits(value);
        return convertInt32ToRegisters(intBits);
    }

    /**
     * 转换双精度浮点数为寄存器
     * @param value
     * @return
     */
    private short[] convertDoubleToRegisters(double value) {
        long longBits = Double.doubleToLongBits(value);

        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(byteOrder);
        buffer.putLong(longBits);
        buffer.flip();

        return new short[]{
                buffer.getShort(),
                buffer.getShort(),
                buffer.getShort(),
                buffer.getShort()
        };
    }

    /**
     * 转换字符串为寄存器
     * @param str
     * @param registerCount
     * @return
     */
    private short[] convertStringToRegisters(String str, int registerCount) {
        short[] registers = new short[registerCount];
        int byteCount = registerCount * 2;
        byte[] bytes = str.getBytes();

        // 确保字节数组长度
        byte[] paddedBytes = new byte[byteCount];
        int length = Math.min(bytes.length, byteCount);
        System.arraycopy(bytes, 0, paddedBytes, 0, length);

        ByteBuffer buffer = ByteBuffer.wrap(paddedBytes);
        buffer.order(byteOrder);

        for (int i = 0; i < registerCount; i++) {
            registers[i] = buffer.getShort();
        }

        return registers;
    }


    /**
     * 按连续地址分组（保证能正确合并批量读取）
     * @param points
     * @return
     */
    private List<List<GroupedPoint>> groupContinuousPoints(List<GroupedPoint> points) {

        if (points == null || points.isEmpty()) {
            return Collections.emptyList();
        }

        // 按地址排序
        points.sort(Comparator.comparingInt(o -> o.getAddress().getAddress()));

        List<List<GroupedPoint>> groups = new ArrayList<>();
        List<GroupedPoint> currentGroup = new ArrayList<>();

        int lastAddress = -999999; // 一个不可能的初始值

        for (GroupedPoint gp : points) {
            int addr = gp.getAddress().getAddress();

            if (currentGroup.isEmpty()) {
                // 第一个地址
                currentGroup.add(gp);
            } else {

                // 如果和上一点连续地址（例如 0,1,2,3）
                if (addr == lastAddress + 1) {
                    currentGroup.add(gp);
                } else {
                    // 不连续 → 结束当前组，开启新组
                    groups.add(currentGroup);
                    currentGroup = new ArrayList<>();
                    currentGroup.add(gp);
                }
            }

            lastAddress = addr;
        }

        // 最后一个组
        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }

        return groups;
    }


    /**
     * 从整数列表创建写多个寄存器请求
     * @param startAddress
     * @param values
     * @return
     */
    public static WriteMultipleRegistersRequest buildWriteMultipleRegisters(
            int startAddress, List<Integer> values) {

        ByteBuffer buffer = ByteBuffer.allocate(values.size() * 2);
        for (Integer value : values) {
            buffer.putShort(value.shortValue());
        }
        buffer.flip();

        return new WriteMultipleRegistersRequest(
                startAddress,
                values.size(),
                buffer.array()
        );
    }


    /**
     * 从short数组创建写多个寄存器请求
     * @param startAddress
     * @param values
     * @return
     */
    public static WriteMultipleRegistersRequest buildWriteMultipleRegisters(
            int startAddress, short[] values) {

        ByteBuffer buffer = ByteBuffer.allocate(values.length * 2);
        for (short value : values) {
            buffer.putShort(value);
        }
        buffer.flip();

        return new WriteMultipleRegistersRequest(
                startAddress,
                values.length,
                buffer.array()
        );
    }
}