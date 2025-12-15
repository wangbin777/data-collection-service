package com.wangbin.collector.core.collector.protocol.modbus;

import com.digitalpetri.modbus.client.ModbusRtuClient;
import com.digitalpetri.modbus.pdu.*;
import com.digitalpetri.modbus.Crc16;
import com.digitalpetri.modbus.serial.client.SerialPortClientTransport;
import com.fazecast.jSerialComm.SerialPort;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.enums.DataType;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.collector.protocol.modbus.domain.GroupedPoint;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusAddress;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusUtils;
import com.wangbin.collector.core.collector.protocol.modbus.domain.RegisterType;
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
 * Modbus RTU采集器（使用modbus-master-tcp库）
 */
@Slf4j
@Component
public class ModbusRtuCollector extends BaseCollector {

    private ModbusRtuClient client;
    private String serialPort;
    private int baudRate;
    private int dataBits;
    private int stopBits;
    private int parity;
    private int slaveId;
    private int timeout;
    private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
    private int interFrameDelay = 5; // 帧间延时(ms)

    private final Map<RegisterType, Map<Integer, DataPoint>> registerCache = new ConcurrentHashMap<>();

    @Override
    public String getCollectorType() {
        return "ModbusRTU";
    }

    @Override
    public String getProtocolType() {
        return "MODBUS_RTU";
    }

    @Override
    protected void doConnect() throws Exception {
        log.info("开始建立 Modbus RTU 连接: {}", deviceInfo.getDeviceId());
        Map<String, Object> protocolConfig = deviceInfo.getProtocolConfig();

        serialPort = (String) protocolConfig.getOrDefault("serialPort", "COM1");
        baudRate = (Integer) protocolConfig.getOrDefault("baudRate", 9600);
        dataBits = (Integer) protocolConfig.getOrDefault("dataBits", 8);
        stopBits = (Integer) protocolConfig.getOrDefault("stopBits", 1);
        parity = (Integer) protocolConfig.getOrDefault("parity", SerialPort.NO_PARITY);
        timeout = (Integer) protocolConfig.getOrDefault("timeout", 3000);
        slaveId = (Integer) protocolConfig.getOrDefault("slaveId", 1);

        var transport = SerialPortClientTransport.create(cfg -> {
            cfg.setSerialPort(serialPort);
            cfg.setBaudRate(baudRate);
            cfg.setDataBits(dataBits);
            cfg.setParity(parity);
            cfg.setStopBits(stopBits);
        });

        this.client = ModbusRtuClient.create(transport);
        this.client.connect();

        log.info(
                "Modbus RTU 连接成功: {} baud={} dataBits={} stopBits={} parity={}",
                serialPort, baudRate, dataBits, stopBits, parity
        );
    }





    @Override
    protected void doDisconnect() throws Exception {
        if (client != null) {
            client.disconnect();
            client = null;
        }
        registerCache.clear();
        log.info("Modbus RTU连接已断开");
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

        // 逐个写入
        for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
            DataPoint point = entry.getKey();
            Object value = entry.getValue();

            try {
                boolean success = doWritePoint(point, value);
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
        log.info("Modbus RTU订阅: 数量={}", points.size());

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
        log.info("取消Modbus RTU订阅: 数量={}", points.size());

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
        status.put("protocol", getProtocolType());
        status.put("serialPort", serialPort);
        status.put("baudRate", baudRate);
        status.put("dataBits", dataBits);
        status.put("stopBits", stopBits);
        status.put("parity", parity);
        status.put("slaveId", slaveId);
        status.put("timeout", timeout);
        status.put("byteOrder", byteOrder.toString());
        status.put("clientConnected", client != null);
        status.put("interFrameDelay", interFrameDelay);

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
            case "READ_EXCEPTION_STATUS" -> executeReadExceptionStatus(params);
            case "DIAGNOSTICS" -> executeDiagnostics(params);
            case "GET_COMM_EVENT_COUNTER" -> executeGetCommEventCounter(params);
            case "GET_COMM_EVENT_LOG" -> executeGetCommEventLog(params);
            default -> throw new IllegalArgumentException("不支持的Modbus命令: " + command);
        };
    }

    // =============== Modbus操作实现 ===============

    /**
     * 读取线圈
     */
    private Boolean readCoil(ModbusAddress address) throws Exception {
        CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(slaveId,
                new ReadCoilsRequest(address.getAddress(), 1));
        // 阻塞等待结果
        ReadCoilsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.getCoilValue(response.coils());
    }

    /**
     * 读取离散输入
     */
    private Boolean readDiscreteInput(ModbusAddress address) throws Exception {
        CompletionStage<ReadDiscreteInputsResponse> future = client.readDiscreteInputsAsync(slaveId,
                new ReadDiscreteInputsRequest(address.getAddress(), 1));
        // 阻塞等待结果
        ReadDiscreteInputsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.getCoilValue(response.inputs());
    }

    /**
     * 读取保持寄存器
     */
    private Object readHoldingRegister(ModbusAddress address, String dataType) throws Exception {
        CompletionStage<ReadHoldingRegistersResponse> future = client.readHoldingRegistersAsync(
                slaveId,
                new ReadHoldingRegistersRequest(address.getAddress(), DataType.fromString(dataType).getRegisterCount())
        );
        ReadHoldingRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
        return ModbusUtils.convertByteToValue(response.registers(), dataType);
    }

    /**
     * 读取输入寄存器
     */
    private Object readInputRegister(ModbusAddress address, String dataType) throws Exception {
        CompletionStage<ReadInputRegistersResponse> future = client.readInputRegistersAsync(
                slaveId,
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
                slaveId,
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
            CompletionStage<WriteSingleRegisterResponse> future = client.writeSingleRegisterAsync(slaveId, request);

            try {
                WriteSingleRegisterResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                return response != null;
            } finally {
                ReferenceCountUtil.release(request);
            }
        } else {
            // 写入多个寄存器
            WriteMultipleRegistersRequest request = buildWriteMultipleRegisters(
                    address.getAddress(), registers);
            CompletionStage<WriteMultipleRegistersResponse> future = client.writeMultipleRegistersAsync(slaveId, request);
            try {
                WriteMultipleRegistersResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                return response != null;
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
     */
    private Map<String, Object> batchReadCoils(List<GroupedPoint> pointGroup, int minAddress, int quantity) throws Exception {
        ReadCoilsRequest request = new ReadCoilsRequest(minAddress, quantity);
        CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(slaveId, request);

        try {
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
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    /**
     * 批量读取离散输入
     */
    private Map<String, Object> batchReadDiscreteInputs(List<GroupedPoint> pointGroup, int minAddress, int quantity) throws Exception {
        ReadDiscreteInputsRequest request = new ReadDiscreteInputsRequest(minAddress, quantity);
        CompletionStage<ReadDiscreteInputsResponse> future = client.readDiscreteInputsAsync(slaveId, request);

        try {
            ReadDiscreteInputsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            Map<String, Object> results = new HashMap<>();

            if (response != null && response.inputs() != null) {
                byte[] raw = response.inputs();
                int bytes = raw.length;

                for (GroupedPoint gp : pointGroup) {
                    int offset = gp.getAddress().getAddress() - minAddress;
                    if (offset < 0 || offset >= quantity) continue;

                    int byteIndex = offset / 8;
                    int bitIndex = offset % 8;

                    if (byteIndex < bytes) {
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

    /**
     * 批量读取保持寄存器
     */
    private Map<String, Object> batchReadHoldingRegisters(List<GroupedPoint> pointGroup, int minAddress, int quantity) throws Exception {
        // 计算本组总共需要读取多少寄存器
        int maxRegisters = pointGroup.stream()
                .mapToInt(gp -> DataType.fromString(gp.getPoint().getDataType()).getRegisterCount())
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
                    int registersNeeded = DataType.fromString(gp.getPoint().getDataType()).getRegisterCount();

                    if ((offsetRegister + registersNeeded) * 2 <= raw.length) {
                        byte[] registerData = Arrays.copyOfRange(raw, offsetRegister * 2, (offsetRegister + registersNeeded) * 2);
                        Object value = ModbusUtils.parseValue(registerData, 0, gp.getPoint().getDataType());
                        results.put(gp.getPoint().getPointId(), value);
                    }
                }
            }

            return results;
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    /**
     * 批量读取输入寄存器
     */
    private Map<String, Object> batchReadInputRegisters(List<GroupedPoint> pointGroup, int minAddress, int quantity) throws Exception {
        // 计算本组总共需要读取多少寄存器
        int maxRegisters = pointGroup.stream()
                .mapToInt(gp -> DataType.fromString(gp.getPoint().getDataType()).getRegisterCount())
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
                    int registersNeeded = DataType.fromString(gp.getPoint().getDataType()).getRegisterCount();

                    if ((offsetRegister + registersNeeded) * 2 <= raw.length) {
                        byte[] registerData = Arrays.copyOfRange(raw, offsetRegister * 2, (offsetRegister + registersNeeded) * 2);
                        Object value = ModbusUtils.parseValue(registerData, 0, gp.getPoint().getDataType());
                        results.put(gp.getPoint().getPointId(), value);
                    }
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
                ByteBuffer buffer = ByteBuffer.wrap(raw);
                buffer.order(ByteOrder.BIG_ENDIAN);

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

        // 构建寄存器数据
        short[] registers = new short[values.size()];
        for (int i = 0; i < values.size(); i++) {
            registers[i] = values.get(i).shortValue();
        }

        WriteMultipleRegistersRequest request = buildWriteMultipleRegisters(address, registers);
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

        ReadCoilsRequest request = new ReadCoilsRequest(address, quantity);
        CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(slaveId, request);

        try {
            ReadCoilsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            List<Boolean> values = ModbusUtils.getCoilValues(response.coils(), quantity);

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

    private Object executeWriteCoils(Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        @SuppressWarnings("unchecked")
        List<Boolean> values = (List<Boolean>) params.get("values");
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("values参数不能为空");
        }

        int quantity = values.size();
        int byteCount = (quantity + 7) / 8;
        byte[] coilBytes = new byte[byteCount];

        for (int i = 0; i < quantity; i++) {
            if (values.get(i)) {
                int byteIndex = i / 8;
                int bitIndex = i % 8;
                coilBytes[byteIndex] |= (1 << bitIndex);
            }
        }

        WriteMultipleCoilsRequest request = new WriteMultipleCoilsRequest(address, quantity, coilBytes);
        CompletionStage<WriteMultipleCoilsResponse> future = client.writeMultipleCoilsAsync(slaveId, request);

        try {
            WriteMultipleCoilsResponse response = future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            return Map.of(
                    "success", response != null,
                    "address", address,
                    "quantity", quantity
            );
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    private Object executeReadExceptionStatus(Map<String, Object> params) throws Exception {
        try {
            // RTU 请求帧：SlaveId + Function + CRC(2)
            byte[] requestData = new byte[4];
            requestData[0] = (byte) slaveId;
            requestData[1] = 0x07; // Read Exception Status

            byte[] crc = calcModbusCrc(requestData, 2);
            requestData[2] = crc[0]; // CRC Lo
            requestData[3] = crc[1]; // CRC Hi

            // 这里你后面直接走串口写 requestData 即可
            // serialPort.writeBytes(requestData, requestData.length);

            return Map.of(
                    "success", true,
                    "request", requestData
            );
        } catch (Exception e) {
            throw new Exception("读取异常状态失败: " + e.getMessage(), e);
        }
    }


    private Object executeDiagnostics(Map<String, Object> params) throws Exception {
        try {
            int subFunction = (int) params.getOrDefault("subFunction", 0x0000);
            int data = (int) params.getOrDefault("data", 0x0000);

            byte[] requestData = new byte[8];
            requestData[0] = (byte) slaveId;
            requestData[1] = 0x08; // Diagnostics
            requestData[2] = (byte) ((subFunction >> 8) & 0xFF);
            requestData[3] = (byte) (subFunction & 0xFF);
            requestData[4] = (byte) ((data >> 8) & 0xFF);
            requestData[5] = (byte) (data & 0xFF);

            byte[] crc = calcModbusCrc(requestData, 6);
            requestData[6] = crc[0]; // CRC Lo
            requestData[7] = crc[1]; // CRC Hi

            // serialPort.writeBytes(requestData, requestData.length);

            return Map.of(
                    "success", true,
                    "request", requestData
            );
        } catch (Exception e) {
            throw new Exception("诊断功能执行失败: " + e.getMessage(), e);
        }
    }

    private byte[] calcModbusCrc(byte[] data, int length) {
        Crc16 crc16 = new Crc16();
        for (int i = 0; i < length; i++) {
            crc16.update(data[i] & 0xFF);
        }
        int crc = crc16.getValue();

        // Modbus RTU: Low byte first
        return new byte[] {
                (byte) (crc & 0xFF),
                (byte) ((crc >> 8) & 0xFF)
        };
    }


    private Object executeGetCommEventCounter(Map<String, Object> params) throws Exception {
        return Map.of(
                "success", false,
                "message", "获取通信事件计数器功能需要底层实现"
        );
    }

    private Object executeGetCommEventLog(Map<String, Object> params) throws Exception {
        return Map.of(
                "success", false,
                "message", "获取通信事件日志功能需要底层实现"
        );
    }

    private Object executeDiagnostic(Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        result.put("protocol", getProtocolType());
        result.put("serialPort", serialPort);
        result.put("baudRate", baudRate);
        result.put("dataBits", dataBits);
        result.put("stopBits", stopBits);
        result.put("parity", parity);
        result.put("slaveId", slaveId);
        result.put("timeout", timeout);
        result.put("byteOrder", byteOrder.toString());
        result.put("clientConnected", client != null);
        result.put("interFrameDelay", interFrameDelay);
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
     */
    private boolean testConnection() throws Exception {
        try {
            CompletionStage<ReadHoldingRegistersResponse> future = client.readHoldingRegistersAsync(
                    slaveId, new ReadHoldingRegistersRequest(0, 1));
            future.toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            log.warn("连接测试失败", e);
            return false;
        }
    }

    /**
     * 解析Modbus地址
     */
    private ModbusAddress parseModbusAddress(String addressStr) {
        if (addressStr == null || addressStr.isEmpty()) {
            throw new IllegalArgumentException("Modbus地址不能为空");
        }

        try {
            int address;
            RegisterType type;

            if (addressStr.contains("x") || addressStr.contains("X") || addressStr.contains(":")) {
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
                int fullAddress = Integer.parseInt(addressStr);
                int typeCode = fullAddress / 10000;
                address = fullAddress % 10000 - 1;

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
     * 转换值为寄存器数组
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
     * 转换32位整数为寄存器
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
     */
    private short[] convertUInt32ToRegisters(long value) {
        return convertInt32ToRegisters((int) value);
    }

    /**
     * 转换浮点数为寄存器
     */
    private short[] convertFloatToRegisters(float value) {
        int intBits = Float.floatToIntBits(value);
        return convertInt32ToRegisters(intBits);
    }

    /**
     * 转换双精度浮点数为寄存器
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
     */
    private short[] convertStringToRegisters(String str, int registerCount) {
        short[] registers = new short[registerCount];
        int byteCount = registerCount * 2;
        byte[] bytes = str.getBytes();

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
     * 按连续地址分组
     */
    private List<List<GroupedPoint>> groupContinuousPoints(List<GroupedPoint> points) {
        if (points == null || points.isEmpty()) {
            return Collections.emptyList();
        }

        points.sort(Comparator.comparingInt(o -> o.getAddress().getAddress()));

        List<List<GroupedPoint>> groups = new ArrayList<>();
        List<GroupedPoint> currentGroup = new ArrayList<>();

        int lastAddress = -999999;

        for (GroupedPoint gp : points) {
            int addr = gp.getAddress().getAddress();

            if (currentGroup.isEmpty()) {
                currentGroup.add(gp);
            } else {
                if (addr == lastAddress + 1) {
                    currentGroup.add(gp);
                } else {
                    groups.add(currentGroup);
                    currentGroup = new ArrayList<>();
                    currentGroup.add(gp);
                }
            }

            lastAddress = addr;
        }

        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }

        return groups;
    }


    /**
     * 构建写多个寄存器请求
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


    private <T> T rtuWait(CompletionStage<T> future) throws Exception {
        T result = future.toCompletableFuture()
                .get(timeout, TimeUnit.MILLISECONDS);

        // RTU 帧间隔，3.5 字符时间
        if (interFrameDelay > 0) {
            Thread.sleep(interFrameDelay);
        }
        return result;
    }
}