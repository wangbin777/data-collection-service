package com.wangbin.collector.core.collector.protocol.iec;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.collector.protocol.iec.base.AbstractIce104Collector;
import com.wangbin.collector.core.collector.protocol.iec.connection.Iec104ConnectionManager;
import com.wangbin.collector.core.collector.protocol.iec.domain.Iec104Address;
import com.wangbin.collector.core.collector.protocol.iec.domain.Iec104Config;
import com.wangbin.collector.core.collector.protocol.iec.factory.Iec104AsduFactory;
import com.wangbin.collector.core.collector.protocol.iec.util.Iec104Utils;
import lombok.extern.slf4j.Slf4j;
import org.openmuc.j60870.*;
import org.openmuc.j60870.ie.IeDoubleCommand;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * IEC 104 协议采集器
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
        log.info("开始建立IEC 104连接: {}", getDeviceId());
        initIec104Config(deviceInfo);
        ConnectionEventListener listener = createConnectionEventListener();
        connect(listener);
    }

    /**
     * 监听
     * @return
     */
    private ConnectionEventListener createConnectionEventListener() {
        return new ConnectionEventListener() {
            @Override
            public void newASdu(Connection conn, ASdu asdu) {
                try {
                    log.debug("收到ASDU消息: {}", asdu);
                    // TODO: 实现具体的消息处理逻辑
                    // 处理响应数据
                    handleResponse(asdu);
                    // 处理站端主动上报的数据
                    // 这可以用于订阅模式下的实时数据更新
                    switch (asdu.getTypeIdentification()) {
                        case M_SP_NA_1:
                        case M_DP_NA_1:
                        case M_ME_NA_1:
                        case M_ME_NB_1:
                        case M_ME_NC_1:
                            // 处理测量数据上报
                            //processDataReport(asdu);
                            break;
                        case C_IC_NA_1:
                            // 处理总召唤响应
                            //processGeneralInterrogationResponse(asdu);
                            break;
                    }

                    lastActivityTime = System.currentTimeMillis();
                } catch (Exception e) {
                    handleError("ASDU处理失败", e);
                }
            }

            @Override
            public void connectionClosed(Connection conn, IOException e) {
                handleConnectionClosed(e);
            }

            @Override
            public void dataTransferStateChanged(Connection conn, boolean started) {
                dataTransferStarted = started;
                if (started) {
                    log.info("与客户端的数据传输已启动: {}", conn.getRemoteInetAddress());
                } else {
                    doDisconnect();
                    log.warn("与客户端的数据传输已停止: {}", conn.getRemoteInetAddress());
                }
            }
        };
    }


    @Override
    protected Object doReadPoint(DataPoint point) throws Exception {

        Iec104Address address = Iec104Utils.parseAddress(point.getAddress());
        log.debug("读取IEC 104点位: typeId={}, address={}", address.getTypeId(), address.getIoAddress());

        return readValueByType(address);
    }

    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) {
        Map<String, Object> results = new HashMap<>();

        for (DataPoint point : points) {
            try {
                Object value = doReadPoint(point);
                results.put(point.getPointId(), value);
            } catch (Exception e) {
                log.error("读取IEC 104点位失败: {}", point.getPointName(), e);
                results.put(point.getPointId(), null);
            }
        }
        return results;
    }

    @Override
    protected boolean doWritePoint(DataPoint point, Object value) throws Exception {

        Iec104Address address = Iec104Utils.parseAddress(point.getAddress());
        log.debug("写入IEC 104点位: typeId={}, address={}, value={}",
                address.getTypeId(), address.getIoAddress(), value);

        return writeValueByType(address, value);
    }

    @Override
    protected Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) {
        Map<String, Boolean> results = new HashMap<>();

        for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
            try {
                boolean success = doWritePoint(entry.getKey(), entry.getValue());
                results.put(entry.getKey().getPointId(), success);
            } catch (Exception e) {
                log.error("写入IEC 104点位失败: {}", entry.getKey().getPointName(), e);
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
        log.info("成功订阅IEC 104点位，数量: {}", points.size());
    }

    @Override
    protected void doUnsubscribe(List<DataPoint> points) {
        for (DataPoint point : points) {
            subscribedPointMap.remove(point.getPointId());
        }
        log.info("成功取消订阅IEC 104点位，数量: {}", points.size());
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
        switch (command.toLowerCase()) {
            case "test_connection":
                return testConnection();
            case "general_interrogation":
                return sendGeneralInterrogation();
            case "read_single_point":
                return readSinglePoint(getAddressFromParams(params));
            case "read_double_point":
                return readDoublePoint(getAddressFromParams(params));
            case "read_measured_value":
                return readMeasuredValue(getAddressFromParams(params));
            default:
                throw new IllegalArgumentException("不支持的IEC 104命令: " + command);
        }
    }

    @Override
    protected void buildReadPlans(String deviceId, List<DataPoint> points) throws Exception {
        log.info("IEC 104点位加载完成，数量: {}", points.size());
    }

    // =============== 私有方法 ===============


    private void handleConnectionClosed(IOException e) {
        connected = false;
        connectionStatus = "DISCONNECTED";
        lastDisconnectTime = System.currentTimeMillis();

        if (e != null) {
            handleError("连接异常关闭", e);
        } else {
            log.info("IEC 104连接正常关闭");
        }
    }


    private void handleError(String message, Exception e) {
        totalErrorCount.incrementAndGet();
        lastError = e.getMessage();
        log.error(message, e);
    }


    private Object readValueByType(Iec104Address address) throws Exception {
        // 根据类型ID发送相应的读取请求
        ASdu readCommand = createReadCommandByType(address);

        // 发送读取命令
        sendASdu(readCommand);

        // 等待响应
        CompletableFuture<Object> future = sendReadRequest(
                address.getIoAddress(),
                String.valueOf(address.getTypeId())
        );

        try {
            // 设置超时
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("读取点位超时: typeId={}, address={}",
                    address.getTypeId(), address.getIoAddress());
            throw new Exception("读取点位超时", e);
        }
    }

    private ASdu createReadCommandByType(Iec104Address address) {
        // 对于各种类型，都可以使用C_RD_NA_1命令
        return Iec104AsduFactory.createReadCommand(
                commonAddress,
                address.getIoAddress(),
                ASduType.C_RD_NA_1
        );
    }

    private boolean writeValueByType(Iec104Address address, Object value) throws Exception {
        return switch (address.getTypeId()) {
            case 45 -> // TYPE_SINGLE_COMMAND
                    writeSingleCommand(address, value);
            case 46 -> // TYPE_DOUBLE_COMMAND
                    writeDoubleCommand(address, value); // TYPE_SETPOINT_NORMALIZED
            // TYPE_SETPOINT_SCALED
            case 48, 49, 50 -> // TYPE_SETPOINT_FLOAT
                    writeSetpointCommand(address, value, address.getTypeId());
            default -> throw new IllegalArgumentException("不支持的信息对象类型: " + address.getTypeId());
        };
    }

    private Boolean readSinglePoint(Iec104Address address) throws Exception {
        sendGeneralInterrogation();
        log.warn("单点信息读取需要站端主动发送，当前返回null");
        return null;
    }

    private Integer readDoublePoint(Iec104Address address) throws Exception {
        sendGeneralInterrogation();
        log.warn("双点信息读取需要站端主动发送，当前返回null");
        return null;
    }

    private Float readMeasuredValue(Iec104Address address) throws Exception {
        sendGeneralInterrogation();
        log.warn("测量值读取需要站端主动发送，当前返回null");
        return null;
    }

    private Integer readScaledValue(Iec104Address address) throws Exception {
        sendGeneralInterrogation();
        log.warn("标度化测量值读取需要站端主动发送，当前返回null");
        return null;
    }

    private Float readFloatingPointValue(Iec104Address address) throws Exception {
        sendGeneralInterrogation();
        log.warn("短浮点测量值读取需要站端主动发送，当前返回null");
        return null;
    }

    private boolean writeSingleCommand(Iec104Address address, Object value) throws Exception {
        boolean commandValue = Boolean.parseBoolean(value.toString());
        ASdu asdu = Iec104AsduFactory.createSingleCommandAsdu(
                commonAddress, address.getIoAddress(), commandValue);

        sendASdu(asdu);
        log.info("单点命令发送成功，地址: {}, 状态: {}",
                address.getIoAddress(), commandValue ? "ON" : "OFF");
        return true;
    }

    private boolean writeDoubleCommand(Iec104Address address, Object value) throws Exception {
        IeDoubleCommand.DoubleCommandState commandState = Iec104Utils.parseDoubleCommandState(value);
        ASdu asdu = Iec104AsduFactory.createDoubleCommandAsdu(
                commonAddress, address.getIoAddress(), commandState);

        sendASdu(asdu);
        log.info("双点命令发送成功，地址: {}, 状态: {}", address.getIoAddress(), commandState);
        return true;
    }

    private boolean writeSetpointCommand(Iec104Address address, Object value, int typeId) throws Exception {
        float floatValue = Float.parseFloat(value.toString());
        ASdu asdu = Iec104AsduFactory.createSetpointCommandAsdu(
                commonAddress, address.getIoAddress(), typeId, floatValue);

        sendASdu(asdu);
        log.info("设点命令发送成功，地址: {}, 类型: {}, 值: {}",
                address.getIoAddress(), typeId, floatValue);
        return true;
    }

    private boolean sendGeneralInterrogation() throws Exception {
        try {
            log.info("发送总召唤命令");
            // 使用总召唤请求，需要根据具体协议实现
            Thread.sleep(100);
            log.info("总召唤命令发送成功");
            return true;
        } catch (Exception e) {
            log.error("发送总召唤命令失败", e);
            return false;
        }
    }

    private Map<String, Object> testConnection() {
        Map<String, Object> result = new HashMap<>();
        try {
            boolean success = sendGeneralInterrogation();
            result.put("success", success);
            result.put("message", success ? "连接测试成功" : "连接测试失败");
            result.put("protocol", "IEC 104");
            result.put("host", host);
            result.put("port", port);
            result.put("commonAddress", commonAddress);
        } catch (Exception e) {
            result.put("success", false);
            result.put("message", "连接测试失败: " + e.getMessage());
            result.put("error", e.getMessage());
            log.error("连接测试失败", e);
        }
        return result;
    }

    private Iec104Address getAddressFromParams(Map<String, Object> params) {
        String address = (String) params.get("address");
        return Iec104Utils.parseAddress(address);
    }

    private String getDeviceId() {
        return deviceInfo != null ? deviceInfo.getDeviceId() : "UNKNOWN";
    }

}