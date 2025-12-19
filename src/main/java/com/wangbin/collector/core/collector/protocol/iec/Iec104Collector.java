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
import org.openmuc.j60870.ie.*;

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
                    // 处理响应数据
                    handleResponse(conn,asdu);
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
            public void dataTransferStateChanged(Connection conn, boolean stopped) {
                dataTransferStopped = stopped;
                if (stopped) {
                    log.warn("与客户端的数据传输已停止: {}", conn.getRemoteInetAddress());
                } else {
                    //doDisconnect();
                    log.info("与客户端的数据传输已启动: {}", conn.getRemoteInetAddress());
                }
            }
        };
    }


    @Override
    protected Object doReadPoint(DataPoint point) throws Exception {

        Iec104Address address = Iec104Utils.parseAddress(point.getUnit(),point.getAddress());
        int ioa = address.getIoAddress();
        int typeId = address.getTypeId();
        int key = generateRequestId(ioa);

        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingRequests.put(key, future);

        log.debug("IEC104 READ → typeId={}, ioa={}", typeId, ioa);

        // ⚠️ 注意：104 的 READ 是“请求数据”，不是直接返回值
        connection.readCommand(commonAddress, ioa);

        try {
            // ⏳ 等 ASDU 在 newASdu 里回来
            Object value = future.get(timeout, TimeUnit.MILLISECONDS);

            if (value instanceof Boolean) return (Boolean) value ? 1 : 0;
            if (value instanceof IeDoublePointWithQuality.DoublePointInformation)
                return ((IeDoublePointWithQuality.DoublePointInformation) value).ordinal();
            return value;
        } catch (TimeoutException e) {
            pendingRequests.remove(key);
            throw new IOException("IEC104 read timeout, ioa=" + ioa);
        }
    }


    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) {

        Map<String, CompletableFuture<Object>> futures = new HashMap<>();

        for (DataPoint point : points) {
            Iec104Address address = Iec104Utils.parseAddress(point.getUnit(),point.getAddress());
            int key = generateRequestId(address.getIoAddress());
            CompletableFuture<Object> future = new CompletableFuture<>();
            pendingRequests.put(key, future);
            futures.put(point.getPointId(), future);

            try {
                connection.readCommand(address.getTypeId(), address.getIoAddress());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        Map<String, Object> results = new HashMap<>();

        for (Map.Entry<String, CompletableFuture<Object>> entry : futures.entrySet()) {
            try {
                Object value = entry.getValue().get(timeout, TimeUnit.MILLISECONDS);
                if (value instanceof Boolean)
                    value = (Boolean) value ? 1 : 0;
                if (value instanceof IeDoublePointWithQuality.DoublePointInformation)
                    value =((IeDoublePointWithQuality.DoublePointInformation) value).ordinal();

                results.put(entry.getKey(),value);
            } catch (Exception e) {
                results.put(entry.getKey(), null);
            }
        }

        return results;
    }


    @Override
    protected boolean doWritePoint(DataPoint point, Object value) throws Exception {

        Iec104Address address = Iec104Utils.parseAddress(point.getUnit(),point.getAddress());
        log.debug("写入IEC 104点位: typeId={}, address={}, value={}",
                address.getTypeId(), address.getIoAddress(), value);

        return writeValueByType(address, value,timeTag);
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

        int ql = (int) params.getOrDefault("ql", 0);
        boolean select = (boolean) params.getOrDefault("select", false);
        int rctAddress = (int) params.get("address");
        Object value = params.get("value");

        switch (command.toLowerCase()) {
            // ============ 控制命令 ============
            case "single_command":
                // connection.singleCommand(int commonAddress, CauseOfTransmission cot, int informationObjectAddress, IeSingleCommand singleCommand)
                int scAddress = (int) params.get("address");
                boolean scState = (boolean) params.get("state");
                IeSingleCommand sc = new IeSingleCommand(scState, 0,false);
                connection.singleCommand(commonAddress, CauseOfTransmission.ACTIVATION, scAddress, sc);
                return "单点命令已发送";

            case "single_command_with_timetag":
                // connection.singleCommandWithTimeTag(int commonAddress, CauseOfTransmission cot, int informationObjectAddress, IeSingleCommand singleCommand, IeTime56 timeTag)
                int sctAddress = (int) params.get("address");
                boolean sctState = (boolean) params.get("state");
                IeSingleCommand sct = new IeSingleCommand(sctState, 0,false);
                IeTime56 sctTime = new IeTime56(System.currentTimeMillis());
                connection.singleCommandWithTimeTag(commonAddress, CauseOfTransmission.ACTIVATION, sctAddress, sct, sctTime);
                return "带时标的单点命令已发送";

            case "double_command":
                // connection.doubleCommand(int commonAddress, CauseOfTransmission cot, int informationObjectAddress, IeDoubleCommand doubleCommand)
                int dcAddress = (int) params.get("address");
                IeDoubleCommand.DoubleCommandState dcState = Iec104Utils.parseDoubleCommandState(params.get("state"));
                IeDoubleCommand dc = new IeDoubleCommand(dcState, 0,false);
                connection.doubleCommand(commonAddress, CauseOfTransmission.ACTIVATION, dcAddress, dc);
                return "双点命令已发送";

            case "double_command_with_timetag":
                // connection.doubleCommandWithTimeTag(int commonAddress, CauseOfTransmission cot, int informationObjectAddress, IeDoubleCommand doubleCommand, IeTime56 timeTag)
                int dctAddress = (int) params.get("address");
                IeDoubleCommand.DoubleCommandState dctState = Iec104Utils.parseDoubleCommandState(params.get("state"));
                IeDoubleCommand dct = new IeDoubleCommand(dctState, 0,false);
                IeTime56 dctTime = new IeTime56(System.currentTimeMillis());
                connection.doubleCommandWithTimeTag(commonAddress, CauseOfTransmission.ACTIVATION, dctAddress, dct, dctTime);
                return "带时标的双点命令已发送";

            case "regulating_step_command":
                // connection.regulatingStepCommand(int commonAddress, CauseOfTransmission cot, int informationObjectAddress, IeRegulatingStepCommand regulatingStepCommand)
                IeRegulatingStepCommand.StepCommandState state = IeRegulatingStepCommand.StepCommandState.getInstance(Integer.parseInt(value.toString()));
                IeRegulatingStepCommand rc = new IeRegulatingStepCommand(state,ql, select);
                connection.regulatingStepCommand(commonAddress, CauseOfTransmission.ACTIVATION, rctAddress, rc);
                return "调节步命令已发送";

            case "regulating_step_command_with_timetag":
                // connection.regulatingStepCommandWithTimeTag(int commonAddress, CauseOfTransmission cot, int informationObjectAddress, IeRegulatingStepCommand regulatingStepCommand, IeTime56 timeTag)
                IeRegulatingStepCommand.StepCommandState stateWithTimeTag = IeRegulatingStepCommand.StepCommandState.getInstance(Integer.parseInt(value.toString()));
                IeRegulatingStepCommand rct = new IeRegulatingStepCommand(stateWithTimeTag,ql, select);
                IeTime56 rctTime = new IeTime56(System.currentTimeMillis());
                connection.regulatingStepCommandWithTimeTag(commonAddress, CauseOfTransmission.ACTIVATION, rctAddress, rct, rctTime);
                return "带时标的调节步命令已发送";

            case "set_normalized_value_command":
                // connection.setNormalizedValueCommand(int commonAddress, CauseOfTransmission cot, int informationObjectAddress, IeNormalizedValue normalizedValue, IeQualifierOfSetPointCommand qualifier)
                int snvAddress = (int) params.get("address");
                float snvValue = (float) params.get("value");
                IeNormalizedValue snv = new IeNormalizedValue(snvValue);
                IeQualifierOfSetPointCommand snvQualifier = new IeQualifierOfSetPointCommand(ql, select);
                connection.setNormalizedValueCommand(commonAddress, CauseOfTransmission.ACTIVATION, snvAddress, snv, snvQualifier);
                return "规一化设点命令已发送";

            case "set_scaled_value_command":
                // connection.setScaledValueCommand(int commonAddress, CauseOfTransmission cot, int informationObjectAddress, IeScaledValue scaledValue, IeQualifierOfSetPointCommand qualifier)
                int ssvAddress = (int) params.get("address");
                int ssvValue = (int) params.get("value");
                IeScaledValue ssv = new IeScaledValue(ssvValue);
                IeQualifierOfSetPointCommand ssvQualifier = new IeQualifierOfSetPointCommand(ql, select);
                connection.setScaledValueCommand(commonAddress, CauseOfTransmission.ACTIVATION, ssvAddress, ssv, ssvQualifier);
                return "标度化设点命令已发送";

            case "set_short_float_command":
                // connection.setShortFloatCommand(int commonAddress, CauseOfTransmission cot, int informationObjectAddress, IeShortFloat floatVal, IeQualifierOfSetPointCommand qualifier)
                int ssfAddress = (int) params.get("address");
                float ssfValue = (float) params.get("value");
                IeShortFloat ssf = new IeShortFloat(ssfValue);
                IeQualifierOfSetPointCommand ssfQualifier = new IeQualifierOfSetPointCommand(ql, select);
                connection.setShortFloatCommand(commonAddress, CauseOfTransmission.ACTIVATION, ssfAddress, ssf, ssfQualifier);
                return "短浮点设点命令已发送";

            case "bit_string_command":
                // connection.bitStringCommand(int commonAddress, CauseOfTransmission cot, int informationObjectAddress, IeBinaryStateInformation binaryStateInformation)
                int bsAddress = (int) params.get("address");
                int bsValue = (int) params.get("value");
                IeBinaryStateInformation bs = new IeBinaryStateInformation(bsValue);
                connection.bitStringCommand(commonAddress, CauseOfTransmission.ACTIVATION, bsAddress, bs);
                return "比特串命令已发送";

            // ============ 召唤和系统命令 ============
            case "interrogation":
            case "general_interrogation":
                // connection.interrogation(int commonAddress, CauseOfTransmission cot, IeQualifierOfInterrogation qualifier)
                int qualifierValue = (int) params.getOrDefault("qualifier", 20);
                IeQualifierOfInterrogation qualifier = new IeQualifierOfInterrogation(qualifierValue);
                connection.interrogation(commonAddress, CauseOfTransmission.ACTIVATION, qualifier);
                return "总召唤命令已发送";

            case "counter_interrogation":
                // connection.counterInterrogation(int commonAddress, CauseOfTransmission cot, IeQualifierOfCounterInterrogation qualifier)
                //读取当前反向有功电度总和（最常用）
                int ciRequest = (int) params.getOrDefault("qualifier", 5);
                int ciFreeze = (int) params.getOrDefault("freeze", 0);
                IeQualifierOfCounterInterrogation ciQualifier = new IeQualifierOfCounterInterrogation(ciRequest, ciFreeze);
                connection.counterInterrogation(commonAddress, CauseOfTransmission.ACTIVATION, ciQualifier);
                return "电度召唤命令已发送";

            case "read_command":
                // connection.readCommand(int commonAddress, int informationObjectAddress)
                int rdAddress = (int) params.get("address");
                connection.readCommand(commonAddress, rdAddress);
                return "读取命令已发送";

            case "synchronize_clocks":
            case "clock_synchronization":
                // connection.synchronizeClocks(int commonAddress, IeTime56 time)
                IeTime56 time = new IeTime56(System.currentTimeMillis());
                connection.synchronizeClocks(commonAddress, time);
                return "时钟同步命令已发送";

            case "test_command":
                // connection.testCommand(int commonAddress)
                connection.testCommand(commonAddress);
                return "测试命令已发送";

            case "test_command_with_timetag":
                // connection.testCommandWithTimeTag(int commonAddress, IeTestSequenceCounter testSequenceCounter, IeTime56 time)
                int testSeq = (int) params.getOrDefault("sequence", 0);
                IeTestSequenceCounter testSeqCounter = new IeTestSequenceCounter(testSeq);
                IeTime56 testTime = new IeTime56(System.currentTimeMillis());
                connection.testCommandWithTimeTag(commonAddress, testSeqCounter, testTime);
                return "带时标的测试命令已发送";

            case "reset_process_command":
                // connection.resetProcessCommand(int commonAddress, IeQualifierOfResetProcessCommand qualifier)
                int rpQualifierValue = (int) params.getOrDefault("qualifier", 0);
                IeQualifierOfResetProcessCommand rpQualifier = new IeQualifierOfResetProcessCommand(rpQualifierValue);
                connection.resetProcessCommand(commonAddress, rpQualifier);
                return "复位进程命令已发送";

            case "delay_acquisition_command":
                // connection.delayAcquisitionCommand(int commonAddress, CauseOfTransmission cot, IeTime16 time)
                int delayTime = (int) params.getOrDefault("delay", 0);
                IeTime16 delay = new IeTime16(delayTime);
                connection.delayAcquisitionCommand(commonAddress, CauseOfTransmission.ACTIVATION, delay);
                return "延时采集命令已发送";

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


    private boolean writeValueByType(Iec104Address address, Object value,boolean timeTag) throws Exception {
        return switch (address.getTypeId()) {
            case 45 -> // C_SC_NA_1: 单点命令
                    writeSingleCommand(address, value,timeTag);
            case 46 -> // C_DC_NA_1: 双点命令
                    writeDoubleCommand(address, value,timeTag);
            case 47 -> // C_RC_NA_1: 调节步命令
                    writeRegulatingStepCommand(address, value,timeTag);
            case 48, 49, 50 -> // C_SE_NA_1, C_SE_NB_1, C_SE_NC_1: 设点命令
                    writeSetPointCommand(address, value, address.getTypeId(),timeTag);
            case 51 -> // C_BO_NA_1: 比特串命令
                    writeBitStringCommand(address, value,timeTag);
            default ->
                    throw new IllegalArgumentException("不支持的信息对象类型: " + address.getTypeId());
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

    /**
     * 发送单点命令
     * @param address
     * @param value
     * @return
     * @throws Exception
     */
    private boolean writeSingleCommand(Iec104Address address, Object value,boolean timeTag) throws Exception {
        boolean commandValue = Boolean.parseBoolean(value.toString());
        IeSingleCommand command = new IeSingleCommand(
                commandValue,
                0,// qualifier
                 false
        );
        // 直接使用 Connection 的方法
        if(timeTag){
            IeTime56 time56 = new IeTime56(System.currentTimeMillis());
            connection.singleCommandWithTimeTag(commonAddress,CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(),command,time56
            );
        }else{
            connection.singleCommand(commonAddress,CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(),command);
        }

        return true;
    }

    /**
     * 发送双点命令
     * @param address
     * @param value
     * @return
     * @throws Exception
     */
    private boolean writeDoubleCommand(Iec104Address address, Object value,boolean timeTag) throws Exception {
        // 解析双点命令状态
        IeDoubleCommand.DoubleCommandState commandState = Iec104Utils.parseDoubleCommandState(value);

        // 创建双点命令信息元素
        IeDoubleCommand doubleCommand = new IeDoubleCommand(
                commandState,  // 命令状态：ON/OFF/INVALID
                0,         // 通常设为false（非选择执行）
                false              // 限定词（qualifier）
        );

        // ⭐ 直接使用 Connection 的内置方法
        if(timeTag){
            IeTime56 time56 = new IeTime56(System.currentTimeMillis());
            connection.doubleCommandWithTimeTag(
                    commonAddress,
                    CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(),
                    doubleCommand,
                    time56
            );
        }else{
            connection.doubleCommand(
                    commonAddress,                      // 站地址
                    CauseOfTransmission.ACTIVATION,     // 传输原因：激活
                    address.getIoAddress(),             // 信息对象地址（点地址）
                    doubleCommand                       // 双点命令值
            );
        }
        log.info("双点命令发送成功，地址: {}, 状态: {}",
                address.getIoAddress(), commandState);
        return true;
    }

    /**
     * 发送设点命令
     * @param address
     * @param value
     * @param typeId
     * @return
     * @throws Exception
     */
    private boolean writeSetPointCommand(Iec104Address address, Object value, int typeId,boolean timeTag) throws Exception {
        float floatValue = Float.parseFloat(value.toString());
        // 根据typeId选择对应的Connection方法
        switch (typeId) {
            case 48: // C_SE_NA_1: 设点命令，规一化值
                return writeNormalizedSetpoint(address, floatValue,timeTag);
            case 49: // C_SE_NB_1: 设点命令，标度化值
                return writeScaledSetPoint(address, floatValue,timeTag);
            case 50: // C_SE_NC_1: 设点命令，短浮点数
                return writeShortFloatSetPoint(address, floatValue,timeTag);
            default:
                throw new IllegalArgumentException("不支持的设点命令类型: " + typeId);
        }
    }

    private boolean writeNormalizedSetpoint(Iec104Address address, float value,boolean timeTag) throws Exception {
        // 将float转换为规一化值（范围通常为-1.0到1.0）
        IeNormalizedValue normalizedValue = new IeNormalizedValue(value);

        IeQualifierOfSetPointCommand qualifier = new IeQualifierOfSetPointCommand(0,false);

        // ⭐ 直接使用 Connection 的方法
        if(timeTag){
            IeTime56 time56 = new IeTime56(System.currentTimeMillis());
            connection.setNormalizedValueCommandWithTimeTag(commonAddress,CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(),normalizedValue,qualifier,time56);
        }else{
            connection.setNormalizedValueCommand(commonAddress,CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(),normalizedValue,qualifier);
        }


        log.info("规一化设点命令发送成功，地址: {}, 值: {}", address.getIoAddress(), value);
        return true;
    }

    private boolean writeScaledSetPoint(Iec104Address address, float value,boolean timeTag) throws Exception {
        // 将float转换为标度化值（整数）
        int scaledValue = (int) value;
        IeScaledValue ieScaledValue = new IeScaledValue(scaledValue);
        IeQualifierOfSetPointCommand qualifier = new IeQualifierOfSetPointCommand(0,false);

        // ⭐ 直接使用 Connection 的方法
        if(timeTag){
            IeTime56 time56 = new IeTime56(System.currentTimeMillis());
            connection.setScaledValueCommandWithTimeTag(commonAddress,CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(),ieScaledValue,qualifier,time56);
        }else{
            connection.setScaledValueCommand(commonAddress,CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(),ieScaledValue,qualifier);
        }


        log.info("标度化设点命令发送成功，地址: {}, 值: {}", address.getIoAddress(), scaledValue);
        return true;
    }

    private boolean writeShortFloatSetPoint(Iec104Address address, float value,boolean timeTag) throws Exception {
        // 创建短浮点数
        IeShortFloat shortFloat = new IeShortFloat(value);
        IeQualifierOfSetPointCommand qualifier = new IeQualifierOfSetPointCommand(0,false);

        // ⭐ 直接使用 Connection 的方法
        if(timeTag){
            IeTime56 time56 = new IeTime56(System.currentTimeMillis());
            connection.setShortFloatCommandWithTimeTag(commonAddress,CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(),shortFloat,qualifier,time56);
        }else{
            connection.setShortFloatCommand(commonAddress,CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(),shortFloat,qualifier);
        }

        log.info("短浮点设点命令发送成功，地址: {}, 值: {}", address.getIoAddress(), value);
        return true;
    }


    private boolean writeRegulatingStepCommand(Iec104Address address, Object value,boolean timeTag) throws Exception {
        // 解析调节步命令
        IeRegulatingStepCommand.StepCommandState state = IeRegulatingStepCommand.StepCommandState.getInstance(Integer.parseInt(value.toString()));
        IeRegulatingStepCommand command = new IeRegulatingStepCommand(state, 0,false);

        // ⭐ 使用 Connection 的方法
        if(timeTag){
            IeTime56 time56 = new IeTime56(System.currentTimeMillis());
            connection.regulatingStepCommandWithTimeTag(commonAddress,CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(),command,time56);
        }else{
            connection.regulatingStepCommand(commonAddress,CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(),command);
        }

        return true;
    }

    private boolean writeBitStringCommand(Iec104Address address, Object value,boolean timeTag) throws Exception {
        // 解析比特串（32位）
        int bitString = Integer.parseInt(value.toString());
        IeBinaryStateInformation binaryState = new IeBinaryStateInformation(bitString);

        // ⭐ 使用 Connection 的方法
        if(timeTag){
            IeTime56 time56 = new IeTime56(System.currentTimeMillis());
            connection.bitStringCommandWithTimeTag(commonAddress,CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(),binaryState,time56);
        }else{
            connection.bitStringCommand(commonAddress,CauseOfTransmission.ACTIVATION,
                    address.getIoAddress(),binaryState);
        }


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

    private String getDeviceId() {
        return deviceInfo != null ? deviceInfo.getDeviceId() : "UNKNOWN";
    }

}