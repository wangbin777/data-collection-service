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
                    log.debug("Receive ASDU: {}", asdu);
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

        Object cached = getCachedValue(ioa);
        if (cached != null) {
            return cached;
        }

        CompletableFuture<Object> future = registerPendingRequest(ioa);

        if (!maybeTriggerSingleInterrogation(point)) {
            connection.readCommand(commonAddress, ioa);
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
        Map<Integer, DataPoint> pending = new HashMap<>();

        for (DataPoint point : points) {
            Iec104Address address = Iec104Utils.parseAddress(resolveCommonAddress(point), point.getAddress());
            Object cached = getCachedValue(address.getIoAddress());
            if (cached != null) {
                results.put(point.getPointId(), cached);
                continue;
            }

            CompletableFuture<Object> future = registerPendingRequest(address.getIoAddress());
            futures.put(point.getPointId(), future);
            pending.put(address.getIoAddress(), point);
        }

        Set<Integer> triggeredQualifiers = new HashSet<>();
        for (Map.Entry<Integer, DataPoint> entry : pending.entrySet()) {
            DataPoint point = entry.getValue();
            Iec104Address address = Iec104Utils.parseAddress(resolveCommonAddress(point), point.getAddress());
            boolean triggered = maybeTriggerSingleInterrogation(point, triggeredQualifiers);
            if (!triggered) {
                try {
                    connection.readCommand(commonAddress, address.getIoAddress());
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
        log.debug("Write IEC104 point: typeId={}, address={}, value={}",
                address.getTypeId(), address.getIoAddress(), value);
        return writeValueByType(address, value, timeTag);
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
        int ql = (int) params.getOrDefault("ql", 0);
        boolean select = (boolean) params.getOrDefault("select", false);
        int rctAddress = (int) params.getOrDefault("address", 0);
        Object value = params.get("value");

        switch (command.toLowerCase()) {
            case "single_command": {
                int address = (int) params.get("address");
                boolean state = (boolean) params.get("state");
                IeSingleCommand sc = new IeSingleCommand(state, 0, false);
                connection.singleCommand(commonAddress, CauseOfTransmission.ACTIVATION, address, sc);
                return "single command sent";
            }
            case "single_command_with_timetag": {
                int address = (int) params.get("address");
                boolean state = (boolean) params.get("state");
                IeSingleCommand sc = new IeSingleCommand(state, 0, false);
                IeTime56 time = new IeTime56(System.currentTimeMillis());
                connection.singleCommandWithTimeTag(commonAddress, CauseOfTransmission.ACTIVATION, address, sc, time);
                return "single command with time tag sent";
            }
            case "double_command": {
                int address = (int) params.get("address");
                IeDoubleCommand.DoubleCommandState state = Iec104Utils.parseDoubleCommandState(params.get("state"));
                IeDoubleCommand dc = new IeDoubleCommand(state, 0, false);
                connection.doubleCommand(commonAddress, CauseOfTransmission.ACTIVATION, address, dc);
                return "double command sent";
            }
            case "double_command_with_timetag": {
                int address = (int) params.get("address");
                IeDoubleCommand.DoubleCommandState state = Iec104Utils.parseDoubleCommandState(params.get("state"));
                IeDoubleCommand dc = new IeDoubleCommand(state, 0, false);
                IeTime56 time = new IeTime56(System.currentTimeMillis());
                connection.doubleCommandWithTimeTag(commonAddress, CauseOfTransmission.ACTIVATION, address, dc, time);
                return "double command with time tag sent";
            }
            case "regulating_step_command": {
                IeRegulatingStepCommand.StepCommandState state =
                        IeRegulatingStepCommand.StepCommandState.getInstance(Integer.parseInt(value.toString()));
                IeRegulatingStepCommand rc = new IeRegulatingStepCommand(state, ql, select);
                connection.regulatingStepCommand(commonAddress, CauseOfTransmission.ACTIVATION, rctAddress, rc);
                return "regulating step sent";
            }
            case "regulating_step_command_with_timetag": {
                IeRegulatingStepCommand.StepCommandState state =
                        IeRegulatingStepCommand.StepCommandState.getInstance(Integer.parseInt(value.toString()));
                IeRegulatingStepCommand rc = new IeRegulatingStepCommand(state, ql, select);
                IeTime56 time = new IeTime56(System.currentTimeMillis());
                connection.regulatingStepCommandWithTimeTag(commonAddress, CauseOfTransmission.ACTIVATION, rctAddress, rc, time);
                return "regulating step with time tag sent";
            }
            case "set_normalized_value_command": {
                int address = (int) params.get("address");
                float val = ((Number) params.get("value")).floatValue();
                IeNormalizedValue normalizedValue = new IeNormalizedValue(val);
                IeQualifierOfSetPointCommand qualifier = new IeQualifierOfSetPointCommand(ql, select);
                connection.setNormalizedValueCommand(commonAddress, CauseOfTransmission.ACTIVATION, address, normalizedValue, qualifier);
                return "normalized set point sent";
            }
            case "set_scaled_value_command": {
                int address = (int) params.get("address");
                int val = ((Number) params.get("value")).intValue();
                IeScaledValue scaledValue = new IeScaledValue(val);
                IeQualifierOfSetPointCommand qualifier = new IeQualifierOfSetPointCommand(ql, select);
                connection.setScaledValueCommand(commonAddress, CauseOfTransmission.ACTIVATION, address, scaledValue, qualifier);
                return "scaled set point sent";
            }
            case "set_short_float_command": {
                int address = (int) params.get("address");
                float val = ((Number) params.get("value")).floatValue();
                IeShortFloat shortFloat = new IeShortFloat(val);
                IeQualifierOfSetPointCommand qualifier = new IeQualifierOfSetPointCommand(ql, select);
                connection.setShortFloatCommand(commonAddress, CauseOfTransmission.ACTIVATION, address, shortFloat, qualifier);
                return "short float set point sent";
            }
            case "bit_string_command": {
                int address = (int) params.get("address");
                int val = ((Number) params.get("value")).intValue();
                IeBinaryStateInformation bs = new IeBinaryStateInformation(val);
                connection.bitStringCommand(commonAddress, CauseOfTransmission.ACTIVATION, address, bs);
                return "bit string command sent";
            }
            case "interrogation":
            case "general_interrogation": {
                int qualifierValue = (int) params.getOrDefault("qualifier", 20);
                IeQualifierOfInterrogation qualifier = new IeQualifierOfInterrogation(qualifierValue);
                connection.interrogation(commonAddress, CauseOfTransmission.ACTIVATION, qualifier);
                return "general interrogation sent";
            }
            case "counter_interrogation": {
                int qualifierValue = (int) params.getOrDefault("qualifier", 5);
                int freeze = (int) params.getOrDefault("freeze", 0);
                IeQualifierOfCounterInterrogation qualifier = new IeQualifierOfCounterInterrogation(qualifierValue, freeze);
                connection.counterInterrogation(commonAddress, CauseOfTransmission.ACTIVATION, qualifier);
                return "counter interrogation sent";
            }
            case "read_command": {
                int address = (int) params.get("address");
                connection.readCommand(commonAddress, address);
                return "read command sent";
            }
            case "synchronize_clocks":
            case "clock_synchronization": {
                IeTime56 time = new IeTime56(System.currentTimeMillis());
                connection.synchronizeClocks(commonAddress, time);
                return "clock synchronization sent";
            }
            case "test_command": {
                connection.testCommand(commonAddress);
                return "test command sent";
            }
            case "test_command_with_timetag": {
                int sequence = (int) params.getOrDefault("sequence", 0);
                IeTestSequenceCounter counter = new IeTestSequenceCounter(sequence);
                IeTime56 time = new IeTime56(System.currentTimeMillis());
                connection.testCommandWithTimeTag(commonAddress, counter, time);
                return "test command with time tag sent";
            }
            case "reset_process_command": {
                int qualifierValue = (int) params.getOrDefault("qualifier", 0);
                IeQualifierOfResetProcessCommand qualifier = new IeQualifierOfResetProcessCommand(qualifierValue);
                connection.resetProcessCommand(commonAddress, qualifier);
                return "reset process command sent";
            }
            case "delay_acquisition_command": {
                int delay = (int) params.getOrDefault("delay", 0);
                IeTime16 ieDelay = new IeTime16(delay);
                connection.delayAcquisitionCommand(commonAddress, CauseOfTransmission.ACTIVATION, ieDelay);
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

    private boolean maybeTriggerSingleInterrogation(DataPoint point) {
        return maybeTriggerSingleInterrogation(point, null);
    }

    private boolean maybeTriggerSingleInterrogation(DataPoint point, Set<Integer> triggered) {
        if (iec104Config == null || !iec104Config.isSingleInterrogationOnReadMiss()) {
            return false;
        }

        Integer qualifier = resolveQualifier(point);
        if (qualifier == null) {
            return false;
        }

        if (triggered != null) {
            if (!triggered.add(qualifier)) {
                return true;
            }
        }

        triggerSingleInterrogation(qualifier, "read-miss");
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

    private boolean writeValueByType(Iec104Address address, Object value, boolean timeTag) throws Exception {
        return switch (address.getTypeId()) {
            case 45 -> writeSingleCommand(address, value, timeTag);
            case 46 -> writeDoubleCommand(address, value, timeTag);
            case 47 -> writeRegulatingStepCommand(address, value, timeTag);
            case 48, 49, 50 -> writeSetPointCommand(address, value, address.getTypeId(), timeTag);
            case 51 -> writeBitStringCommand(address, value, timeTag);
            default -> throw new IllegalArgumentException("Unsupported type: " + address.getTypeId());
        };
    }

    private boolean writeSingleCommand(Iec104Address address, Object value, boolean timeTag) throws Exception {
        boolean commandValue = Boolean.parseBoolean(value.toString());
        IeSingleCommand command = new IeSingleCommand(commandValue, 0, false);
        if (timeTag) {
            IeTime56 time = new IeTime56(System.currentTimeMillis());
            connection.singleCommandWithTimeTag(commonAddress, CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), command, time);
        } else {
            connection.singleCommand(commonAddress, CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), command);
        }
        return true;
    }

    private boolean writeDoubleCommand(Iec104Address address, Object value, boolean timeTag) throws Exception {
        IeDoubleCommand.DoubleCommandState state = Iec104Utils.parseDoubleCommandState(value);
        IeDoubleCommand command = new IeDoubleCommand(state, 0, false);
        if (timeTag) {
            IeTime56 time = new IeTime56(System.currentTimeMillis());
            connection.doubleCommandWithTimeTag(commonAddress, CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), command, time);
        } else {
            connection.doubleCommand(commonAddress, CauseOfTransmission.ACTIVATION,
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
            connection.setNormalizedValueCommandWithTimeTag(commonAddress, CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), normalizedValue, qualifier, time);
        } else {
            connection.setNormalizedValueCommand(commonAddress, CauseOfTransmission.ACTIVATION,
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
            connection.setScaledValueCommandWithTimeTag(commonAddress, CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), scaled, qualifier, time);
        } else {
            connection.setScaledValueCommand(commonAddress, CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), scaled, qualifier);
        }
        return true;
    }

    private boolean writeShortFloatSetPoint(Iec104Address address, float value, boolean timeTag) throws Exception {
        IeShortFloat shortFloat = new IeShortFloat(value);
        IeQualifierOfSetPointCommand qualifier = new IeQualifierOfSetPointCommand(0, false);
        if (timeTag) {
            IeTime56 time = new IeTime56(System.currentTimeMillis());
            connection.setShortFloatCommandWithTimeTag(commonAddress, CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), shortFloat, qualifier, time);
        } else {
            connection.setShortFloatCommand(commonAddress, CauseOfTransmission.ACTIVATION,
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
            connection.regulatingStepCommandWithTimeTag(commonAddress, CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), command, time);
        } else {
            connection.regulatingStepCommand(commonAddress, CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), command);
        }
        return true;
    }

    private boolean writeBitStringCommand(Iec104Address address, Object value, boolean timeTag) throws Exception {
        int bitString = Integer.parseInt(value.toString());
        IeBinaryStateInformation bits = new IeBinaryStateInformation(bitString);
        if (timeTag) {
            IeTime56 time = new IeTime56(System.currentTimeMillis());
            connection.bitStringCommandWithTimeTag(commonAddress, CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(), bits, time);
        } else {
            connection.bitStringCommand(commonAddress, CauseOfTransmission.ACTIVATION,
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
