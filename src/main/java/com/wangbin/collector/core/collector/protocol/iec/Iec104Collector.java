package com.wangbin.collector.core.collector.protocol.iec;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.iec.base.AbstractIce104Collector;
import com.wangbin.collector.core.collector.protocol.iec.domain.Iec104Address;
import com.wangbin.collector.core.collector.protocol.iec.util.Iec104Utils;
import lombok.extern.slf4j.Slf4j;
import org.openmuc.j60870.*;
import org.openmuc.j60870.ie.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * IEC-60870-5-104 collector.
 */
@Slf4j
public class Iec104Collector extends AbstractIce104Collector {

    @Override
    public String getCollectorType() {
        return "IEC104";
    }

    @Override
    public String getProtocolType() {
        return "IEC104";
    }

    @Override
    protected void doConnect() throws Exception {
        log.info("Connecting IEC 104 device: {}", getDeviceId());
        initIec104Config(deviceInfo);
        ConnectionEventListener listener = createConnectionEventListener();
        connect(listener);
    }

    private ConnectionEventListener createConnectionEventListener() {
        return new ConnectionEventListener() {
            @Override
            public void newASdu(Connection conn, ASdu asdu) {
                try {
                    //log.debug("Receive ASDU: {}", asdu);
                    handleResponse(conn, asdu);
                    lastActivityTime = System.currentTimeMillis();
                } catch (Exception e) {
                    handleError("ASDU handling failed", e);
                }
            }

            @Override
            public void connectionClosed(Connection conn, IOException e) {
                handleConnectionClosed(e);
            }

            @Override
            public void dataTransferStateChanged(Connection conn, boolean stopped) {
                dataTransferStopped = stopped;
                if (stopped) {
                    log.warn("IEC104 data transfer stopped: {}", conn.getRemoteInetAddress());
                } else {
                    log.info("IEC104 data transfer started: {}", conn.getRemoteInetAddress());
                }
            }
        };
    }

    @Override
    protected Object doReadPoint(DataPoint point) throws Exception {
        Iec104Address address = Iec104Utils.parseAddress(resolveCommonAddress(point), point.getAddress());
        int ioa = address.getIoAddress();
        int ca = address.getCommonAddress();

        Object cached = getCachedValue(ca, ioa);
        if (cached != null) {
            return cached;
        }

        CompletableFuture<Object> future = registerPendingRequest(ca, ioa);

        if (!maybeTriggerSingleInterrogation(point, address)) {
            connection.readCommand(ca, ioa);
        }

        try {
            Object value = future.get(timeout, TimeUnit.MILLISECONDS);
            if (value instanceof Boolean) {
                return (Boolean) value ? 1 : 0;
            }
            if (value instanceof IeDoublePointWithQuality.DoublePointInformation dpi) {
                return dpi.ordinal();
            }
            return value;
        } catch (TimeoutException e) {
            throw new IOException("IEC104 read timeout, ioa=" + ioa);
        }
    }

    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) {
        Map<String, Object> results = new HashMap<>();
        Map<String, CompletableFuture<Object>> futures = new HashMap<>();
        Map<Iec104Key, DataPoint> pendingPoints = new LinkedHashMap<>();
        Map<Iec104Key, Iec104Address> pendingAddresses = new HashMap<>();

        for (DataPoint point : points) {
            Iec104Address address = Iec104Utils.parseAddress(resolveCommonAddress(point), point.getAddress());
            Object cached = getCachedValue(address.getCommonAddress(), address.getIoAddress());
            if (cached != null) {
                results.put(point.getPointId(), cached);
                continue;
            }

            CompletableFuture<Object> future = registerPendingRequest(address.getCommonAddress(), address.getIoAddress());
            futures.put(point.getPointId(), future);

            Iec104Key key = new Iec104Key(address.getCommonAddress(), address.getIoAddress());
            pendingPoints.putIfAbsent(key, point);
            pendingAddresses.putIfAbsent(key, address);
        }

        Set<InterrogationKey> triggeredQualifiers = new HashSet<>();
        for (Map.Entry<Iec104Key, DataPoint> entry : pendingPoints.entrySet()) {
            Iec104Key key = entry.getKey();
            DataPoint point = entry.getValue();
            Iec104Address address = pendingAddresses.get(key);
            boolean triggered = maybeTriggerSingleInterrogation(point, address, triggeredQualifiers);
            if (!triggered) {
                try {
                    connection.readCommand(key.commonAddress(), key.ioAddress());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        for (Map.Entry<String, CompletableFuture<Object>> entry : futures.entrySet()) {
            try {
                Object value = entry.getValue().get(timeout, TimeUnit.MILLISECONDS);
                if (value instanceof Boolean) {
                    value = (Boolean) value ? 1 : 0;
                }
                if (value instanceof IeDoublePointWithQuality.DoublePointInformation dpi) {
                    value = dpi.ordinal();
                }
                results.put(entry.getKey(), value);
            } catch (Exception e) {
                results.put(entry.getKey(), null);
            }
        }

        return results;
    }

    @Override
    protected boolean doWritePoint(DataPoint point, Object value) throws Exception {
        Iec104Address address = Iec104Utils.parseAddress(resolveCommonAddress(point), point.getAddress());
        int typeId = resolveWriteTypeId(point, address);
        log.debug("Write IEC104 point: typeId={}, ca={}, address={}, value={}",
                typeId, address.getCommonAddress(), address.getIoAddress(), value);
        return writeValueByType(address, typeId, value, timeTag);
    }

    @Override
    protected Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) {
        Map<String, Boolean> results = new HashMap<>();
        for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
            try {
                boolean success = doWritePoint(entry.getKey(), entry.getValue());
                results.put(entry.getKey().getPointId(), success);
            } catch (Exception e) {
                log.error("Write IEC104 point failed: {}", entry.getKey().getPointName(), e);
                results.put(entry.getKey().getPointId(), false);
            }
        }
        return results;
    }

    @Override
    protected void doSubscribe(List<DataPoint> points) {
        for (DataPoint point : points) {
            subscribedPointMap.put(point.getPointId(), point);
        }
        log.info("Subscribed IEC104 points, size={}", points.size());
    }

    @Override
    protected void doUnsubscribe(List<DataPoint> points) {
        for (DataPoint point : points) {
            subscribedPointMap.remove(point.getPointId());
        }
        log.info("Unsubscribed IEC104 points, size={}", points.size());
    }

    @Override
    protected Map<String, Object> doGetDeviceStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("protocol", "IEC 104");
        status.put("host", host);
        status.put("port", port);
        status.put("commonAddress", commonAddress);
        status.put("timeout", timeout);
        status.put("connected", isConnected());
        status.put("connectionStatus", connectionStatus);
        status.put("subscribedPoints", subscribedPointsSet.size());
        status.put("lastConnectTime", lastConnectTime);
        status.put("lastActivityTime", lastActivityTime);
        status.put("totalErrorCount", totalErrorCount.get());
        status.put("lastError", lastError);
        return status;
    }

    @Override
    protected Object doExecuteCommand(int unitId, String command, Map<String, Object> params) throws Exception {
        int ca = resolveCommandCommonAddress(unitId, params);
        int ql = (int) params.getOrDefault("ql", 0);
        boolean select = (boolean) params.getOrDefault("select", false);
        int rctAddress = (int) params.getOrDefault("address", 0);
        Object value = params.get("value");

        switch (command.toLowerCase()) {
            case "single_command": {
                int address = (int) params.get("address");
                boolean state = (boolean) params.get("state");
                IeSingleCommand sc = new IeSingleCommand(state, 0, false);
                connection.singleCommand(ca, CauseOfTransmission.ACTIVATION, address, sc);
                return "single command sent";
            }
            case "single_command_with_timetag": {
                int address = (int) params.get("address");
                boolean state = (boolean) params.get("state");
                IeSingleCommand sc = new IeSingleCommand(state, 0, false);
                IeTime56 time = new IeTime56(System.currentTimeMillis());
                connection.singleCommandWithTimeTag(ca, CauseOfTransmission.ACTIVATION, address, sc, time);
                return "single command with time tag sent";
            }
            case "double_command": {
                int address = (int) params.get("address");
                IeDoubleCommand.DoubleCommandState state = Iec104Utils.parseDoubleCommandState(params.get("state"));
                IeDoubleCommand dc = new IeDoubleCommand(state, 0, false);
                connection.doubleCommand(ca, CauseOfTransmission.ACTIVATION, address, dc);
                return "double command sent";
            }
            case "double_command_with_timetag": {
                int address = (int) params.get("address");
                IeDoubleCommand.DoubleCommandState state = Iec104Utils.parseDoubleCommandState(params.get("state"));
                IeDoubleCommand dc = new IeDoubleCommand(state, 0, false);
                IeTime56 time = new IeTime56(System.currentTimeMillis());
                connection.doubleCommandWithTimeTag(ca, CauseOfTransmission.ACTIVATION, address, dc, time);
                return "double command with time tag sent";
            }
            case "regulating_step_command": {
                IeRegulatingStepCommand.StepCommandState state =
                        IeRegulatingStepCommand.StepCommandState.getInstance(Integer.parseInt(value.toString()));
                IeRegulatingStepCommand rc = new IeRegulatingStepCommand(state, ql, select);
                connection.regulatingStepCommand(ca, CauseOfTransmission.ACTIVATION, rctAddress, rc);
                return "regulating step sent";
            }
            case "regulating_step_command_with_timetag": {
                IeRegulatingStepCommand.StepCommandState state =
                        IeRegulatingStepCommand.StepCommandState.getInstance(Integer.parseInt(value.toString()));
                IeRegulatingStepCommand rc = new IeRegulatingStepCommand(state, ql, select);
                IeTime56 time = new IeTime56(System.currentTimeMillis());
                connection.regulatingStepCommandWithTimeTag(ca, CauseOfTransmission.ACTIVATION, rctAddress, rc, time);
                return "regulating step with time tag sent";
            }
            case "set_normalized_value_command": {
                int address = (int) params.get("address");
                float val = ((Number) params.get("value")).floatValue();
                IeNormalizedValue normalizedValue = new IeNormalizedValue(val);
                IeQualifierOfSetPointCommand qualifier = new IeQualifierOfSetPointCommand(ql, select);
                connection.setNormalizedValueCommand(ca, CauseOfTransmission.ACTIVATION, address, normalizedValue, qualifier);
                return "normalized set point sent";
            }
            case "set_scaled_value_command": {
                int address = (int) params.get("address");
                int val = ((Number) params.get("value")).intValue();
                IeScaledValue scaledValue = new IeScaledValue(val);
                IeQualifierOfSetPointCommand qualifier = new IeQualifierOfSetPointCommand(ql, select);
                connection.setScaledValueCommand(ca, CauseOfTransmission.ACTIVATION, address, scaledValue, qualifier);
                return "scaled set point sent";
            }
            case "set_short_float_command": {
                int address = (int) params.get("address");
                float val = ((Number) params.get("value")).floatValue();
                IeShortFloat shortFloat = new IeShortFloat(val);
                IeQualifierOfSetPointCommand qualifier = new IeQualifierOfSetPointCommand(ql, select);
                connection.setShortFloatCommand(ca, CauseOfTransmission.ACTIVATION, address, shortFloat, qualifier);
                return "short float set point sent";
            }
            case "bit_string_command": {
                int address = (int) params.get("address");
                int val = ((Number) params.get("value")).intValue();
                IeBinaryStateInformation bs = new IeBinaryStateInformation(val);
                connection.bitStringCommand(ca, CauseOfTransmission.ACTIVATION, address, bs);
                return "bit string command sent";
            }
            case "interrogation":
            case "general_interrogation": {
                int qualifierValue = (int) params.getOrDefault("qualifier", 20);
                IeQualifierOfInterrogation qualifier = new IeQualifierOfInterrogation(qualifierValue);
                connection.interrogation(ca, CauseOfTransmission.ACTIVATION, qualifier);
                return "general interrogation sent";
            }
            case "counter_interrogation": {
                int qualifierValue = (int) params.getOrDefault("qualifier", 5);
                int freeze = (int) params.getOrDefault("freeze", 0);
                IeQualifierOfCounterInterrogation qualifier = new IeQualifierOfCounterInterrogation(qualifierValue, freeze);
                connection.counterInterrogation(ca, CauseOfTransmission.ACTIVATION, qualifier);
                return "counter interrogation sent";
            }
            case "read_command": {
                int address = (int) params.get("address");
                connection.readCommand(ca, address);
                return "read command sent";
            }
            case "synchronize_clocks":
            case "clock_synchronization": {
                IeTime56 time = new IeTime56(System.currentTimeMillis());
                connection.synchronizeClocks(ca, time);
                return "clock synchronization sent";
            }
            case "test_command": {
                connection.testCommand(ca);
                return "test command sent";
            }
            case "test_command_with_timetag": {
                int sequence = (int) params.getOrDefault("sequence", 0);
                IeTestSequenceCounter counter = new IeTestSequenceCounter(sequence);
                IeTime56 time = new IeTime56(System.currentTimeMillis());
                connection.testCommandWithTimeTag(ca, counter, time);
                return "test command with time tag sent";
            }
            case "reset_process_command": {
                int qualifierValue = (int) params.getOrDefault("qualifier", 0);
                IeQualifierOfResetProcessCommand qualifier = new IeQualifierOfResetProcessCommand(qualifierValue);
                connection.resetProcessCommand(ca, qualifier);
                return "reset process command sent";
            }
            case "delay_acquisition_command": {
                int delay = (int) params.getOrDefault("delay", 0);
                IeTime16 ieDelay = new IeTime16(delay);
                connection.delayAcquisitionCommand(ca, CauseOfTransmission.ACTIVATION, ieDelay);
                return "delay acquisition command sent";
            }
            default:
                throw new IllegalArgumentException("Unsupported IEC104 command: " + command);
        }
    }

    @Override
    protected void buildReadPlans(String deviceId, List<DataPoint> points) {
        log.info("IEC104 points loaded, size={}", points.size());
    }

    private void handleConnectionClosed(IOException e) {
        connected = false;
        connectionStatus = "DISCONNECTED";
        lastDisconnectTime = System.currentTimeMillis();

        if (e != null) {
            handleError("Connection closed with error", e);
        } else {
            log.info("IEC104 connection closed");
        }
    }

    private void handleError(String message, Exception e) {
        totalErrorCount.incrementAndGet();
        lastError = e.getMessage();
        log.error(message, e);
    }

    private boolean maybeTriggerSingleInterrogation(DataPoint point, Iec104Address address) {
        return maybeTriggerSingleInterrogation(point, address, null);
    }

    private boolean maybeTriggerSingleInterrogation(DataPoint point,
                                                    Iec104Address address,
                                                    Set<InterrogationKey> triggered) {
        if (iec104Config == null || !iec104Config.isSingleInterrogationOnReadMiss()) {
            return false;
        }
        if (address == null) {
            return false;
        }

        Integer qualifier = resolveQualifier(point);
        if (qualifier == null) {
            return false;
        }

        InterrogationKey key = new InterrogationKey(address.getCommonAddress(), qualifier);
        if (triggered != null) {
            if (!triggered.add(key)) {
                return true;
            }
        }

        triggerSingleInterrogation(key.commonAddress(), key.qualifier(), "read-miss");
        return true;
    }

    private Integer resolveQualifier(DataPoint point) {
        if (point == null || iec104Config == null) {
            return null;
        }
        String field = Optional.ofNullable(iec104Config.getSingleInterrogationGroupField())
                .filter(s -> !s.isBlank())
                .orElse("groupId");
        String raw;
        switch (field) {
            case "unit":
                raw = point.getUnit();
                break;
            case "unitId":
                raw = point.getUnitId() != null ? point.getUnitId().toString() : null;
                break;
            case "pointCode":
                raw = point.getPointCode();
                break;
            case "groupId":
            default:
                raw = point.getGroupId();
                break;
        }
        return resolveSingleInterrogationQualifier(raw).orElse(null);
    }

    private String resolveCommonAddress(DataPoint point) {
        Object cfg = point.getAdditionalConfig("commonAddress");
        if (cfg != null) {
            return cfg.toString();
        }
        if (point.getUnitId() != null) {
            return point.getUnitId().toString();
        }
        return String.valueOf(commonAddress);
    }

    private int resolveWriteTypeId(DataPoint point, Iec104Address address) {
        Integer typeId = address.getTypeId();
        if (typeId == null) {
            typeId = extractConfiguredTypeId(point);
        }
        if (typeId == null) {
            String pointName = point != null ? point.getPointName() : "unknown";
            throw new IllegalArgumentException("IEC104写入缺少typeId, point=" + pointName);
        }
        return typeId;
    }

    private Integer extractConfiguredTypeId(DataPoint point) {
        if (point == null) {
            return null;
        }
        Integer configured = parseInteger(point.getAdditionalConfig("typeId"));
        if (configured != null) {
            return configured;
        }
        configured = parseInteger(point.getAdditionalConfig("iecTypeId"));
        if (configured != null) {
            return configured;
        }
        configured = parseInteger(point.getAdditionalConfig("writeTypeId"));
        if (configured != null) {
            return configured;
        }
        Object registerType = point.getAdditionalConfig("registerType");
        if (registerType != null) {
            return mapRegisterTypeToTypeId(registerType.toString());
        }
        return null;
    }

    private Integer mapRegisterTypeToTypeId(String raw) {
        if (raw == null) {
            return null;
        }
        return switch (raw.trim().toUpperCase()) {
            case "SINGLE_COMMAND", "C_SC_NA_1" -> 45;
            case "DOUBLE_COMMAND", "C_DC_NA_1" -> 46;
            case "REGULATING_STEP", "REGULATING_STEP_COMMAND", "STEP_COMMAND", "C_RC_NA_1" -> 47;
            case "SETPOINT_NORMALIZED", "SET_POINT_NORMALIZED", "C_SE_NA_1" -> 48;
            case "SETPOINT_SCALED", "SET_POINT_SCALED", "C_SE_NB_1" -> 49;
            case "SETPOINT_SHORT_FLOAT", "SETPOINT_FLOAT", "C_SE_NC_1" -> 50;
            case "BITSTRING", "BIT_STRING", "C_BO_NA_1" -> 51;
            default -> null;
        };
    }

    private int resolveCommandCommonAddress(int unitId, Map<String, Object> params) {
        Integer override = null;
        if (params != null) {
            override = parseInteger(params.get("commonAddress"));
            if (override == null) {
                override = parseInteger(params.get("ca"));
            }
            if (override == null) {
                override = parseInteger(params.get("unitId"));
            }
        }
        if (override != null) {
            return override;
        }
        if (unitId > 0) {
            return unitId;
        }
        return commonAddress;
    }

    private Integer parseInteger(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof Number number) {
            return number.intValue();
        }
        String text = raw.toString();
        if (text == null || text.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(text.trim());
        } catch (NumberFormatException e) {
            log.debug("Invalid IEC104 integer: {}", text);
            return null;
        }
    }

    private boolean writeValueByType(Iec104Address address, int typeId, Object value, boolean timeTag) throws Exception {
        return switch (typeId) {
            case 45 -> writeSingleCommand(address, value, timeTag);
            case 46 -> writeDoubleCommand(address, value, timeTag);
            case 47 -> writeRegulatingStepCommand(address, value, timeTag);
            case 48, 49, 50 -> writeSetPointCommand(address, value, typeId, timeTag);
            case 51 -> writeBitStringCommand(address, value, timeTag);
            default -> throw new IllegalArgumentException("Unsupported type: " + typeId);
        };
    }

    private boolean writeSingleCommand(Iec104Address address, Object value, boolean timeTag) throws Exception {
        boolean commandValue = Boolean.parseBoolean(value.toString());
        IeSingleCommand command = new IeSingleCommand(commandValue, 0, false);
        if (timeTag) {
            IeTime56 time = new IeTime56(System.currentTimeMillis());
            connection.singleCommandWithTimeTag(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), command, time);
        } else {
            connection.singleCommand(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), command);
        }
        return true;
    }

    private boolean writeDoubleCommand(Iec104Address address, Object value, boolean timeTag) throws Exception {
        IeDoubleCommand.DoubleCommandState state = Iec104Utils.parseDoubleCommandState(value);
        IeDoubleCommand command = new IeDoubleCommand(state, 0, false);
        if (timeTag) {
            IeTime56 time = new IeTime56(System.currentTimeMillis());
            connection.doubleCommandWithTimeTag(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), command, time);
        } else {
            connection.doubleCommand(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), command);
        }
        return true;
    }

    private boolean writeSetPointCommand(Iec104Address address, Object value, int typeId, boolean timeTag) throws Exception {
        float floatValue = Float.parseFloat(value.toString());
        return switch (typeId) {
            case 48 -> writeNormalizedSetpoint(address, floatValue, timeTag);
            case 49 -> writeScaledSetPoint(address, floatValue, timeTag);
            case 50 -> writeShortFloatSetPoint(address, floatValue, timeTag);
            default -> throw new IllegalArgumentException("Unsupported set point type: " + typeId);
        };
    }

    private boolean writeNormalizedSetpoint(Iec104Address address, float value, boolean timeTag) throws Exception {
        IeNormalizedValue normalizedValue = new IeNormalizedValue(value);
        IeQualifierOfSetPointCommand qualifier = new IeQualifierOfSetPointCommand(0, false);
        if (timeTag) {
            IeTime56 time = new IeTime56(System.currentTimeMillis());
            connection.setNormalizedValueCommandWithTimeTag(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), normalizedValue, qualifier, time);
        } else {
            connection.setNormalizedValueCommand(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), normalizedValue, qualifier);
        }
        return true;
    }

    private boolean writeScaledSetPoint(Iec104Address address, float value, boolean timeTag) throws Exception {
        int scaledValue = (int) value;
        IeScaledValue scaled = new IeScaledValue(scaledValue);
        IeQualifierOfSetPointCommand qualifier = new IeQualifierOfSetPointCommand(0, false);
        if (timeTag) {
            IeTime56 time = new IeTime56(System.currentTimeMillis());
            connection.setScaledValueCommandWithTimeTag(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), scaled, qualifier, time);
        } else {
            connection.setScaledValueCommand(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), scaled, qualifier);
        }
        return true;
    }

    private boolean writeShortFloatSetPoint(Iec104Address address, float value, boolean timeTag) throws Exception {
        IeShortFloat shortFloat = new IeShortFloat(value);
        IeQualifierOfSetPointCommand qualifier = new IeQualifierOfSetPointCommand(0, false);
        if (timeTag) {
            IeTime56 time = new IeTime56(System.currentTimeMillis());
            connection.setShortFloatCommandWithTimeTag(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), shortFloat, qualifier, time);
        } else {
            connection.setShortFloatCommand(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), shortFloat, qualifier);
        }
        return true;
    }

    private boolean writeRegulatingStepCommand(Iec104Address address, Object value, boolean timeTag) throws Exception {
        IeRegulatingStepCommand.StepCommandState state =
                IeRegulatingStepCommand.StepCommandState.getInstance(Integer.parseInt(value.toString()));
        IeRegulatingStepCommand command = new IeRegulatingStepCommand(state, 0, false);
        if (timeTag) {
            IeTime56 time = new IeTime56(System.currentTimeMillis());
            connection.regulatingStepCommandWithTimeTag(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), command, time);
        } else {
            connection.regulatingStepCommand(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), command);
        }
        return true;
    }

    private boolean writeBitStringCommand(Iec104Address address, Object value, boolean timeTag) throws Exception {
        int bitString = Integer.parseInt(value.toString());
        IeBinaryStateInformation bits = new IeBinaryStateInformation(bitString);
        if (timeTag) {
            IeTime56 time = new IeTime56(System.currentTimeMillis());
            connection.bitStringCommandWithTimeTag(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), bits, time);
        } else {
            connection.bitStringCommand(address.getCommonAddress(), CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), bits);
        }
        return true;
    }

    private boolean sendGeneralInterrogation() throws Exception {
        try {
            log.info("Send general interrogation");
            triggerSingleInterrogation(20, "manual");
            return true;
        } catch (Exception e) {
            log.error("Send general interrogation failed", e);
            return false;
        }
    }

    private Map<String, Object> testConnection() {
        Map<String, Object> result = new HashMap<>();
        try {
            boolean success = sendGeneralInterrogation();
            result.put("success", success);
            result.put("message", success ? "connection test success" : "connection test failed");
            result.put("protocol", "IEC 104");
            result.put("host", host);
            result.put("port", port);
            result.put("commonAddress", commonAddress);
        } catch (Exception e) {
            result.put("success", false);
            result.put("message", "connection test failed: " + e.getMessage());
            result.put("error", e.getMessage());
            log.error("connection test failed", e);
        }
        return result;
    }

    private String getDeviceId() {
        return deviceInfo != null ? deviceInfo.getDeviceId() : "UNKNOWN";
    }
}
