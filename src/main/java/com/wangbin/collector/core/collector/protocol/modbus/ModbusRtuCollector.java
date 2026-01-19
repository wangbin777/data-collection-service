package com.wangbin.collector.core.collector.protocol.modbus;

import com.digitalpetri.modbus.client.ModbusRtuClient;
import com.digitalpetri.modbus.pdu.*;
import com.digitalpetri.modbus.serial.client.SerialPortClientTransport;
import com.fazecast.jSerialComm.SerialPort;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.common.enums.DataType;
import com.wangbin.collector.common.enums.Parity;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.collector.protocol.modbus.base.AbstractModbusCollector;
import com.wangbin.collector.core.collector.protocol.modbus.domain.GroupedPoint;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusAddress;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusRequestBuilder;
import com.wangbin.collector.core.collector.protocol.modbus.domain.RegisterType;
import com.wangbin.collector.core.collector.protocol.modbus.plan.ModbusReadPlan;
import com.wangbin.collector.core.collector.protocol.modbus.plan.PointOffset;
import com.wangbin.collector.core.collector.protocol.modbus.utils.ModbusGroupingUtil;
import com.wangbin.collector.core.collector.protocol.modbus.utils.ModbusUtils;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;

/**
 * Modbus RTU采集器（使用modbus-master-tcp库）
 */
@Slf4j
@Component
public class ModbusRtuCollector extends AbstractModbusCollector {

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
    private final Semaphore tcpSemaphore = new Semaphore(1);
    private final Map<RegisterType, Map<Integer, DataPoint>> registerCache = new ConcurrentHashMap<>();
    private static final int MAX_WRITE_REGISTERS = 123;
    private static final int MAX_WRITE_COILS = 1968;

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
        DeviceConnection connectionConfig = deviceInfo.getConnectionConfig();

        interFrameDelay = connectionConfig.getInterFrameDelay();
        serialPort = connectionConfig.getSerialPort();
        baudRate = connectionConfig.getBaudRate();
        dataBits = connectionConfig.getDataBits();
        stopBits = connectionConfig.getStopBits();
        parity = Parity.fromName(connectionConfig.getParity()).getValue();
        timeout = connectionConfig.getReadTimeout();
        slaveId = connectionConfig.getSlaveId();

        var transport = SerialPortClientTransport.create(cfg -> {
            cfg.setSerialPort(serialPort);
            cfg.setBaudRate(baudRate);
            cfg.setDataBits(dataBits);
            cfg.setParity(parity);
            cfg.setStopBits(stopBits);
        });

        client = ModbusRtuClient.create(transport);
        client.connect();

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
        int unitId = sanitizeUnitId(resolveUnitId(point));

        // 根据寄存器类型读取数据
        return switch (modbusAddress.getRegisterType()) {
            case COIL -> readCoil(unitId, modbusAddress);
            case DISCRETE_INPUT -> readDiscreteInput(unitId, modbusAddress);
            case HOLDING_REGISTER -> readHoldingRegister(unitId, modbusAddress, point.getDataType());
            case INPUT_REGISTER -> readInputRegister(unitId, modbusAddress, point.getDataType());
            default -> throw new IllegalArgumentException("不支持的Modbus寄存器类型: " +
                    modbusAddress.getRegisterType());
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
                        Object value = ModbusUtils.parseValue(raw,po.getOffset(),
                                DataType.valueOf(po.getDataType()));
                        results.put(po.getPointId(), value);
                    }
                }

            } catch (Exception e) {
                log.error("ReadPlan 执行失败: unitId={}, type={}, addr={}",
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
     * 执行计划
     * @param plan
     * @return
     * @throws Exception
     */
    private byte[] executeReadPlan(ModbusReadPlan plan) throws Exception {

        return switch (plan.getRegisterType()) {

            case COIL -> {
                var resp = client.readCoilsAsync(plan.getUnitId(),
                        new ReadCoilsRequest(plan.getStartAddress(),plan.getQuantity())
                ).toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                yield resp.coils();
            }

            case DISCRETE_INPUT -> {
                var resp = client.readDiscreteInputsAsync(plan.getUnitId(),
                        new ReadDiscreteInputsRequest(plan.getStartAddress(),plan.getQuantity())
                ).toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                yield resp.inputs();
            }

            case HOLDING_REGISTER -> {
                var resp = client.readHoldingRegistersAsync(plan.getUnitId(),
                        new ReadHoldingRegistersRequest(plan.getStartAddress(),plan.getQuantity())
                ).toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                yield resp.registers();
            }

            case INPUT_REGISTER -> {
                var resp = client.readInputRegistersAsync(plan.getUnitId(),
                        new ReadInputRegistersRequest(plan.getStartAddress(),plan.getQuantity())
                ).toCompletableFuture().get(timeout, TimeUnit.MILLISECONDS);
                yield resp.registers();
            }
        };
    }


    @Override
    protected boolean doWritePoint(DataPoint point, Object value) throws Exception {
        String address = point.getAddress();
        if (address == null || address.isEmpty()) {
            throw new IllegalArgumentException("点位地址不能为空");
        }

        ModbusAddress modbusAddress = parseModbusAddress(address);
        int unitId = sanitizeUnitId(resolveUnitId(point));

        return switch (modbusAddress.getRegisterType()) {
            case COIL -> writeCoil(unitId, modbusAddress, (Boolean) value);
            case HOLDING_REGISTER -> writeHoldingRegister(unitId, modbusAddress, value, point.getDataType());
            default -> throw new IllegalArgumentException("该寄存器类型不支持写入: " +
                    modbusAddress.getRegisterType());
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

                int unitId = sanitizeUnitId(resolveUnitId(point));
                int registerCount = DataType.fromString(point.getDataType()).getRegisterCount();
                BatchKey key = new BatchKey(unitId, type);
                grouped.computeIfAbsent(key, k -> new ArrayList<>())
                        .add(new WriteEntry(point, value, address.getAddress(), registerCount));
            } catch (Exception e) {
                log.error("解析写入点位失败: {}", point.getPointName(), e);
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
            boolean connected = testConnection(slaveId);
            status.put("deviceConnected", connected);
        } catch (Exception e) {
            status.put("deviceConnected", false);
            status.put("connectionError", e.getMessage());
        }

        return status;
    }

    @Override
    protected Object doExecuteCommand(int unitId,String command, Map<String, Object> params) throws Exception {
        Map<String, Object> safeParams = params != null ? params : Collections.emptyMap();
        int targetUnitId = safeParams.containsKey("slaveId") ? sanitizeUnitId(unitId) : slaveId;

        return switch (command.toUpperCase()) {
            case "READ_MULTIPLE_REGISTERS" -> executeReadMultipleRegisters(targetUnitId, safeParams);
            case "WRITE_MULTIPLE_REGISTERS" -> executeWriteMultipleRegisters(targetUnitId, safeParams);
            case "READ_COILS" -> executeReadCoils(targetUnitId, safeParams);
            case "WRITE_COILS" -> executeWriteCoils(targetUnitId, safeParams);
            case "DIAGNOSTIC" -> executeDiagnostic(targetUnitId, safeParams);
            case "READ_EXCEPTION_STATUS" -> executeReadExceptionStatus(targetUnitId, safeParams);
            case "DIAGNOSTICS" -> executeDiagnostics(targetUnitId, safeParams);
            case "GET_COMM_EVENT_COUNTER" -> executeGetCommEventCounter(targetUnitId, safeParams);
            case "GET_COMM_EVENT_LOG" -> executeGetCommEventLog(targetUnitId, safeParams);
            default -> throw new IllegalArgumentException("不支持的Modbus命令: " + command);
        };
    }

    // =============== Modbus操作实现 ===============

    /**
     * 读取线圈
     */
    private Boolean readCoil(int unitId, ModbusAddress address) throws Exception {
        CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(unitId,
                new ReadCoilsRequest(address.getAddress(), 1));
        // 阻塞等待结果
        ReadCoilsResponse response = rtuWait(future);
        return ModbusUtils.parseCoilValue(response.coils(), 0);
    }

    /**
     * 读取离散输入
     */
    private Boolean readDiscreteInput(int unitId, ModbusAddress address) throws Exception {
        CompletionStage<ReadDiscreteInputsResponse> future = client.readDiscreteInputsAsync(unitId,
                new ReadDiscreteInputsRequest(address.getAddress(), 1));
        // 阻塞等待结果
        ReadDiscreteInputsResponse response = rtuWait(future);
        return ModbusUtils.parseCoilValue(response.inputs(), 0);
    }

    /**
     * 读取保持寄存器
     */
    private Object readHoldingRegister(int unitId, ModbusAddress address, String dataType) throws Exception {
        // 使用工具类获取寄存器数量
        int registerCount = DataType.fromString(dataType).getRegisterCount();

        CompletionStage<ReadHoldingRegistersResponse> future = client.readHoldingRegistersAsync(
                unitId,
                new ReadHoldingRegistersRequest(address.getAddress(), registerCount)
        );
        ReadHoldingRegistersResponse response = rtuWait(future);
        return ModbusUtils.convertByteToValue(response.registers(), dataType);
    }

    /**
     * 读取输入寄存器
     */
    private Object readInputRegister(int unitId, ModbusAddress address, String dataType) throws Exception {
        // 使用工具类获取寄存器数量
        int registerCount = DataType.fromString(dataType).getRegisterCount();

        CompletionStage<ReadInputRegistersResponse> future = client.readInputRegistersAsync(
                unitId,
                new ReadInputRegistersRequest(address.getAddress(), registerCount)
        );
        ReadInputRegistersResponse response = rtuWait(future);
        return ModbusUtils.convertByteToValue(response.registers(), dataType);
    }

    /**
     * 写入线圈
     */
    private boolean writeCoil(int unitId, ModbusAddress address, boolean value) throws Exception {
        CompletionStage<WriteSingleCoilResponse> future = client.writeSingleCoilAsync(
                unitId,
                new WriteSingleCoilRequest(address.getAddress(), value)
        );
        WriteSingleCoilResponse response = rtuWait(future);
        return response != null;
    }

    /**
     * 写入保持寄存器
     */
    private boolean writeHoldingRegister(int unitId, ModbusAddress address, Object value, String dataType) throws Exception {
        // 使用工具类获取寄存器数量和转换值
        int registerCount = DataType.fromString(dataType).getRegisterCount();
        short[] registers = ModbusUtils.valueToRegisters(value, dataType, byteOrder);

        if (registerCount == 1) {
            // 写入单个寄存器
            WriteSingleRegisterRequest request = new WriteSingleRegisterRequest(address.getAddress(), registers[0]);
            CompletionStage<WriteSingleRegisterResponse> future = client.writeSingleRegisterAsync(unitId, request);

            try {
                WriteSingleRegisterResponse response = rtuWait(future);
                return response != null;
            } finally {
                ReferenceCountUtil.release(request);
            }
        } else {
            // 写入多个寄存器
            // 使用工具类构建请求数据
            byte[] registerData = ModbusUtils.buildWriteRegistersData(registers);
            WriteMultipleRegistersRequest request = new WriteMultipleRegistersRequest(
                    address.getAddress(), registerCount, registerData);
            CompletionStage<WriteMultipleRegistersResponse> future = client.writeMultipleRegistersAsync(unitId, request);
            try {
                WriteMultipleRegistersResponse response = rtuWait(future);
                return response != null;
            } finally {
                ReferenceCountUtil.release(request);
            }
        }
    }

    // =============== 命令执行方法 ===============

    private Object executeReadMultipleRegisters(int unitId, Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        int quantity = (int) params.getOrDefault("quantity", 1);

        ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(address, quantity);
        CompletionStage<ReadHoldingRegistersResponse> future = client.readHoldingRegistersAsync(unitId, request);

        try {
            ReadHoldingRegistersResponse response = rtuWait(future);
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

    private Object executeWriteMultipleRegisters(int unitId, Map<String, Object> params) throws Exception {
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

        // 使用工具类构建请求数据
        byte[] registerData = ModbusUtils.buildWriteRegistersData(registers);
        WriteMultipleRegistersRequest request = new WriteMultipleRegistersRequest(
                address, registers.length, registerData);
        CompletionStage<WriteMultipleRegistersResponse> future = client.writeMultipleRegistersAsync(unitId, request);

        try {
            WriteMultipleRegistersResponse response = rtuWait(future);
            return Map.of(
                    "success", response != null,
                    "address", address,
                    "quantity", values.size()
            );
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    private Object executeReadCoils(int unitId, Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        int quantity = (int) params.getOrDefault("quantity", 1);

        ReadCoilsRequest request = new ReadCoilsRequest(address, quantity);
        CompletionStage<ReadCoilsResponse> future = client.readCoilsAsync(unitId, request);

        try {
            ReadCoilsResponse response = rtuWait(future);
            // 使用工具类解析线圈值
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

    private Object executeWriteCoils(int unitId, Map<String, Object> params) throws Exception {
        int address = (int) params.getOrDefault("address", 0);
        @SuppressWarnings("unchecked")
        List<Boolean> values = (List<Boolean>) params.get("values");
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("values参数不能为空");
        }

        int quantity = values.size();
        // 使用工具类构建线圈字节数组
        byte[] coilBytes = ModbusUtils.buildCoilBytes(values);

        WriteMultipleCoilsRequest request = new WriteMultipleCoilsRequest(address, quantity, coilBytes);
        CompletionStage<WriteMultipleCoilsResponse> future = client.writeMultipleCoilsAsync(unitId, request);

        try {
            WriteMultipleCoilsResponse response = rtuWait(future);
            return Map.of(
                    "success", response != null,
                    "address", address,
                    "quantity", quantity
            );
        } finally {
            ReferenceCountUtil.release(request);
        }
    }

    private Object executeReadExceptionStatus(int unitId, Map<String, Object> params) throws Exception {
        try {
            // 使用工具类构建RTU请求帧
            byte[] requestData = ModbusUtils.buildRtuExceptionStatusRequest(unitId);

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

    private Object executeDiagnostics(int unitId, Map<String, Object> params) throws Exception {
        try {
            int subFunction = (int) params.getOrDefault("subFunction", 0x0000);
            int data = (int) params.getOrDefault("data", 0x0000);

            // 使用工具类构建RTU诊断请求
            byte[] requestData = ModbusUtils.buildRtuDiagnosticRequest(unitId, subFunction, data);

            // serialPort.writeBytes(requestData, requestData.length);

            return Map.of(
                    "success", true,
                    "request", requestData
            );
        } catch (Exception e) {
            throw new Exception("诊断功能执行失败: " + e.getMessage(), e);
        }
    }

    private Object executeGetCommEventCounter(int unitId, Map<String, Object> params) throws Exception {
        return Map.of(
                "success", false,
                "message", "获取通信事件计数器功能需要底层实现"
        );
    }

    private Object executeGetCommEventLog(int unitId, Map<String, Object> params) throws Exception {
        return Map.of(
                "success", false,
                "message", "获取通信事件日志功能需要底层实现"
        );
    }

    private Object executeDiagnostic(int unitId, Map<String, Object> params) {
        Map<String, Object> result = new HashMap<>();
        result.put("protocol", getProtocolType());
        result.put("serialPort", serialPort);
        result.put("baudRate", baudRate);
        result.put("dataBits", dataBits);
        result.put("stopBits", stopBits);
        result.put("parity", parity);
        result.put("slaveId", unitId);
        result.put("timeout", timeout);
        result.put("byteOrder", byteOrder.toString());
        result.put("clientConnected", client != null);
        result.put("interFrameDelay", interFrameDelay);
        result.put("timestamp", System.currentTimeMillis());

        // 测试连接
        try {
            boolean connected = testConnection(unitId);
            result.put("deviceConnected", connected);
            result.put("connectionTest", "SUCCESS");
        } catch (Exception e) {
            result.put("deviceConnected", false);
            result.put("connectionTest", "FAILED");
            result.put("error", e.getMessage());
        }

        return result;
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
                log.error("单点写入失败: {}", entry.point().getPointName(), e);
                results.put(entry.point().getPointId(), false);
            }
        }
    }

    private boolean writeCoilChunk(int unitId, int startAddress, List<WriteEntry> chunk) {
        try {
            List<Boolean> values = new ArrayList<>();
            for (WriteEntry entry : chunk) {
                values.add(asBoolean(entry.value()));
            }
            byte[] coilBytes = ModbusUtils.buildCoilBytes(values);
            WriteMultipleCoilsRequest request = new WriteMultipleCoilsRequest(startAddress, values.size(), coilBytes);
            CompletionStage<WriteMultipleCoilsResponse> future = client.writeMultipleCoilsAsync(unitId, request);
            WriteMultipleCoilsResponse response = rtuWait(future);
            return response != null;
        } catch (Exception e) {
            log.error("批量写线圈失败: unitId={}, startAddress={}", unitId, startAddress, e);
            return false;
        }
    }

    private boolean writeHoldingChunk(int unitId, int startAddress, List<WriteEntry> chunk) {
        short[] registers = buildRegisterBuffer(chunk);
        WriteMultipleRegistersRequest request = ModbusRequestBuilder.buildWriteMultipleRegisters(
                startAddress,
                registers
        );
        CompletionStage<WriteMultipleRegistersResponse> future = client.writeMultipleRegistersAsync(unitId, request);
        try {
            WriteMultipleRegistersResponse response = rtuWait(future);
            return response != null;
        } catch (Exception e) {
            log.error("批量写保持寄存器失败: unitId={}, startAddress={}", unitId, startAddress, e);
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
                    byteOrder
            );
            System.arraycopy(values, 0, buffer, offset, values.length);
            offset += values.length;
        }
        return buffer;
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
        throw new IllegalArgumentException("无法转换为布尔值: " + value);
    }

    // =============== 辅助方法 ===============

    private int sanitizeUnitId(Integer unitIdValue) {
        return unitIdValue != null && unitIdValue > 0 ? unitIdValue : slaveId;
    }

    private record BatchKey(int unitId, RegisterType registerType) {
    }

    private record WriteEntry(DataPoint point,
                              Object value,
                              int address,
                              int registerCount) {
    }

    /**
     * 测试连接
     */
    private boolean testConnection() throws Exception {
        return testConnection(slaveId);
    }

    private boolean testConnection(int unitId) throws Exception {
        try {
            CompletionStage<ReadHoldingRegistersResponse> future = client.readHoldingRegistersAsync(
                    unitId, new ReadHoldingRegistersRequest(0, 1));
            rtuWait(future);
            return true;
        } catch (Exception e) {
            log.warn("连接测试失败", e);
            return false;
        }
    }

    /**
     * RTU等待方法（带帧间隔）
     */
    private <T> T rtuWait(CompletionStage<T> future) throws Exception {
        T result = future.toCompletableFuture()
                .get(timeout, TimeUnit.MILLISECONDS);

        // RTU 帧间隔，3.5 字符时间
        if (interFrameDelay > 0) {
            Thread.sleep(interFrameDelay);
        }
        return result;
    }

    @Override
    public boolean isConnected() {
        return client != null;
    }
}
