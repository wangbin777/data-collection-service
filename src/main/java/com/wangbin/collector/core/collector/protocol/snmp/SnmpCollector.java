package com.wangbin.collector.core.collector.protocol.snmp;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.common.exception.CollectorException;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.config.CollectorProperties;
import lombok.extern.slf4j.Slf4j;
import org.snmp4j.*;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.*;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * SNMP协议采集器
 */
@Slf4j
@Component
public class SnmpCollector extends BaseCollector {

    private Snmp snmp;
    private CommunityTarget target;
    private TransportMapping<UdpAddress> transport;
    private ScheduledExecutorService scheduler;
    private Map<String, ScheduledFuture<?>> pollingTasks = new ConcurrentHashMap<>();

    @Override
    public String getCollectorType() {
        return "SNMP";
    }

    @Override
    public String getProtocolType() {
        return "SNMP";
    }

    @Override
    protected void doConnect() throws Exception {
        log.info("开始建立SNMP连接: {}", deviceInfo.getDeviceId());

        // 使用配置属性
        CollectorProperties.SnmpConfig snmpConfig = collectorProperties.getSnmp();

        // 优先使用设备特定的配置，没有则使用全局配置
        String ip = (String) config.getOrDefault("ipAddress", "127.0.0.1");
        int port = (Integer) config.getOrDefault("port", 161);
        String community = (String) config.getOrDefault("community", snmpConfig.getCommunity());
        int timeout = (Integer) config.getOrDefault("timeout", snmpConfig.getTimeout());
        int retries = (Integer) config.getOrDefault("retries", snmpConfig.getRetries());
        String versionStr = (String) config.getOrDefault("version", snmpConfig.getVersion());

        // 创建传输映射
        transport = new DefaultUdpTransportMapping();
        transport.listen();

        // 创建SNMP实例
        snmp = new Snmp(transport);

        // 创建目标地址
        Address targetAddress = new UdpAddress(ip + "/" + port);
        target = new CommunityTarget();
        target.setCommunity(new OctetString(community));
        target.setAddress(targetAddress);
        target.setRetries(retries);
        target.setTimeout(timeout);

        // 设置版本
        int version;
        switch (versionStr) {
            case "1":
                version = SnmpConstants.version1;
                break;
            case "3":
                version = SnmpConstants.version3;
                break;
            case "2c":
            default:
                version = SnmpConstants.version2c;
                break;
        }
        target.setVersion(version);

        // 创建调度器
        scheduler = Executors.newScheduledThreadPool(2);

        log.info("SNMP连接参数: IP={}, Port={}, Community={}, Version={}, Timeout={}ms",
                ip, port, community, versionStr, timeout);
    }

    @Override
    protected void doDisconnect() throws Exception {
        log.info("断开SNMP连接: {}", deviceInfo.getDeviceId());

        // 停止所有轮询任务
        for (ScheduledFuture<?> task : pollingTasks.values()) {
            task.cancel(true);
        }
        pollingTasks.clear();

        if (scheduler != null) {
            scheduler.shutdown();
            scheduler = null;
        }

        if (snmp != null) {
            snmp.close();
            snmp = null;
        }

        if (transport != null) {
            transport.close();
            transport = null;
        }

        target = null;
    }

    @Override
    protected Object doReadPoint(DataPoint point) throws Exception {
        String oid = point.getAddress();
        log.debug("读取SNMP点位: OID={}", oid);

        long startTime = System.currentTimeMillis();

        // 创建PDU
        PDU pdu = new PDU();
        pdu.add(new VariableBinding(new OID(oid)));
        pdu.setType(PDU.GET);

        // 发送请求
        ResponseEvent response = snmp.send(pdu, target);

        long responseTime = System.currentTimeMillis() - startTime;

        if (response == null || response.getResponse() == null) {
            throw new IOException("SNMP请求超时或无响应");
        }

        PDU responsePDU = response.getResponse();
        if (responsePDU.getErrorStatus() != PDU.noError) {
            throw new IOException("SNMP错误: " + responsePDU.getErrorStatusText());
        }

        // 获取响应值
        VariableBinding vb = responsePDU.get(0);
        Variable variable = vb.getVariable();

        // 更新统计信息
        totalBytesRead.addAndGet(estimateSize(variable));
        totalReadTime.addAndGet(responseTime);

        // 转换为Java对象
        return convertVariableToJava(variable);
    }

    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) throws Exception {
        log.debug("批量读取SNMP点位: 数量={}", points.size());

        long startTime = System.currentTimeMillis();

        // 创建PDU
        PDU pdu = new PDU();
        Map<Integer, DataPoint> indexMap = new HashMap<>();

        for (int i = 0; i < points.size(); i++) {
            DataPoint point = points.get(i);
            pdu.add(new VariableBinding(new OID(point.getAddress())));
            indexMap.put(i, point);
        }
        pdu.setType(PDU.GET);

        // 发送请求
        ResponseEvent response = snmp.send(pdu, target);

        long responseTime = System.currentTimeMillis() - startTime;

        if (response == null || response.getResponse() == null) {
            throw new IOException("SNMP批量请求超时或无响应");
        }

        PDU responsePDU = response.getResponse();
        if (responsePDU.getErrorStatus() != PDU.noError) {
            throw new IOException("SNMP批量错误: " + responsePDU.getErrorStatusText());
        }

        // 处理响应
        Map<String, Object> results = new HashMap<>();
        for (int i = 0; i < responsePDU.size(); i++) {
            VariableBinding vb = responsePDU.get(i);
            Variable variable = vb.getVariable();
            DataPoint point = indexMap.get(i);

            if (point != null) {
                results.put(point.getPointId(), convertVariableToJava(variable));

                // 更新统计信息
                totalBytesRead.addAndGet(estimateSize(variable));
            }
        }

        // 更新批量读取统计
        totalReadTime.addAndGet(responseTime);

        return results;
    }

    @Override
    protected boolean doWritePoint(DataPoint point, Object value) throws Exception {
        // SNMP写入通常使用SET操作
        String oid = point.getAddress();
        log.debug("写入SNMP点位: OID={}, 值={}", oid, value);

        long startTime = System.currentTimeMillis();

        // 创建PDU
        PDU pdu = new PDU();
        Variable variable = convertJavaToVariable(value, point.getDataType());
        pdu.add(new VariableBinding(new OID(oid), variable));
        pdu.setType(PDU.SET);

        // 发送请求
        ResponseEvent response = snmp.send(pdu, target);

        long responseTime = System.currentTimeMillis() - startTime;

        if (response == null || response.getResponse() == null) {
            throw new IOException("SNMP写入请求超时或无响应");
        }

        PDU responsePDU = response.getResponse();
        boolean success = responsePDU.getErrorStatus() == PDU.noError;

        // 更新统计信息
        if (success) {
            totalWriteTime.addAndGet(responseTime);
            totalBytesWrite.addAndGet(estimateSize(variable));
        }

        return success;
    }

    @Override
    protected Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) throws Exception {
        log.debug("批量写入SNMP点位: 数量={}", points.size());

        long startTime = System.currentTimeMillis();

        // 创建PDU
        PDU pdu = new PDU();
        Map<Integer, DataPoint> indexMap = new HashMap<>();
        int index = 0;

        for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
            DataPoint point = entry.getKey();
            Object value = entry.getValue();

            Variable variable = convertJavaToVariable(value, point.getDataType());
            pdu.add(new VariableBinding(new OID(point.getAddress()), variable));
            indexMap.put(index++, point);

            // 更新写入数据量统计
            totalBytesWrite.addAndGet(estimateSize(variable));
        }
        pdu.setType(PDU.SET);

        // 发送请求
        ResponseEvent response = snmp.send(pdu, target);

        long responseTime = System.currentTimeMillis() - startTime;

        if (response == null || response.getResponse() == null) {
            throw new IOException("SNMP批量写入请求超时或无响应");
        }

        PDU responsePDU = response.getResponse();
        boolean success = responsePDU.getErrorStatus() == PDU.noError;

        // 返回结果
        Map<String, Boolean> results = new HashMap<>();
        for (DataPoint point : points.keySet()) {
            results.put(point.getPointId(), success);
        }

        // 更新统计信息
        if (success) {
            totalWriteTime.addAndGet(responseTime);
        }

        return results;
    }

    @Override
    protected void doSubscribe(List<DataPoint> points) throws Exception {
        log.info("创建SNMP订阅: 数量={}", points.size());

        // SNMP协议不支持原生订阅，使用轮询模拟订阅
        if (points.isEmpty()) {
            return;
        }

        // 为每个设备创建一个轮询任务
        String taskKey = deviceInfo.getDeviceId() + "_polling";

        if (pollingTasks.containsKey(taskKey)) {
            log.warn("SNMP轮询任务已存在: {}", taskKey);
            return;
        }
        // 使用配置属性
        CollectorProperties.SnmpConfig snmpConfig = collectorProperties.getSnmp();
        // 获取轮询间隔
        int pollingInterval = snmpConfig.getPollingInterval();

        ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(() -> {
            try {
                // 执行批量读取
                Map<String, Object> values = doReadPoints(points);

                // 处理数据变化
                for (DataPoint point : points) {
                    Object value = values.get(point.getPointId());
                    if (value != null) {
                        handlePollingData(point.getPointId(), value);
                    }
                }
            } catch (Exception e) {
                log.error("SNMP轮询任务执行失败", e);
            }
        }, 0, pollingInterval, TimeUnit.MILLISECONDS);

        pollingTasks.put(taskKey, task);
        log.info("SNMP轮询任务已启动，间隔: {}ms", pollingInterval);
    }

    @Override
    protected void doUnsubscribe(List<DataPoint> points) throws Exception {
        log.info("取消SNMP订阅: 数量={}", points.size());

        if (points.isEmpty()) {
            // 停止所有轮询任务
            for (ScheduledFuture<?> task : pollingTasks.values()) {
                task.cancel(true);
            }
            pollingTasks.clear();
        } else {
            // 对于SNMP，由于是轮询模拟，需要完全停止任务
            // 或者可以维护一个订阅列表，在轮询时过滤
            log.warn("SNMP订阅不支持部分取消，将停止所有轮询");
            for (ScheduledFuture<?> task : pollingTasks.values()) {
                task.cancel(true);
            }
            pollingTasks.clear();
        }
    }

    @Override
    protected Map<String, Object> doGetDeviceStatus() throws Exception {
        Map<String, Object> status = new HashMap<>();

        status.put("deviceId", deviceInfo.getDeviceId());
        status.put("connected", isConnected());
        status.put("pollingTasks", pollingTasks.size());

        if (!isConnected()) {
            return status;
        }

        try {
            long startTime = System.currentTimeMillis();

            // 尝试读取系统描述来检查设备状态
            PDU pdu = new PDU();
            pdu.add(new VariableBinding(new OID(".1.3.6.1.2.1.1.1.0"))); // sysDescr
            pdu.setType(PDU.GET);

            ResponseEvent response = snmp.send(pdu, target);
            long responseTime = System.currentTimeMillis() - startTime;

            if (response != null && response.getResponse() != null) {
                PDU responsePDU = response.getResponse();
                boolean reachable = responsePDU.getErrorStatus() == PDU.noError;

                status.put("reachable", reachable);
                status.put("responseTime", responseTime);

                if (reachable && responsePDU.size() > 0) {
                    VariableBinding vb = responsePDU.get(0);
                    status.put("sysDescr", vb.getVariable().toString());
                }

                // 读取系统运行时间
                try {
                    pdu = new PDU();
                    pdu.add(new VariableBinding(new OID(".1.3.6.1.2.1.1.3.0"))); // sysUpTime
                    pdu.setType(PDU.GET);

                    response = snmp.send(pdu, target);
                    if (response != null && response.getResponse() != null) {
                        PDU uptimePDU = response.getResponse();
                        if (uptimePDU.getErrorStatus() == PDU.noError && uptimePDU.size() > 0) {
                            VariableBinding vb = uptimePDU.get(0);
                            status.put("sysUpTime", vb.getVariable().toString());
                        }
                    }
                } catch (Exception e) {
                    log.debug("读取sysUpTime失败", e);
                }
            } else {
                status.put("reachable", false);
                status.put("error", "No response from device");
            }
        } catch (Exception e) {
            status.put("reachable", false);
            status.put("error", e.getMessage());
            log.error("获取SNMP设备状态失败", e);
        }

        // 添加轮询任务状态
        List<Map<String, Object>> taskStatus = new ArrayList<>();
        for (Map.Entry<String, ScheduledFuture<?>> entry : pollingTasks.entrySet()) {
            Map<String, Object> taskInfo = new HashMap<>();
            taskInfo.put("taskKey", entry.getKey());
            taskInfo.put("cancelled", entry.getValue().isCancelled());
            taskInfo.put("done", entry.getValue().isDone());
            taskStatus.add(taskInfo);
        }
        status.put("taskStatus", taskStatus);

        return status;
    }

    @Override
    protected Object doExecuteCommand(String command, Map<String, Object> params) throws Exception {
        switch (command.toLowerCase()) {
            case "get":
                return executeGetCommand(params);
            case "walk":
                return executeWalkCommand(params);
            case "getnext":
                return executeGetNextCommand(params);
            case "getbulk":
                return executeGetBulkCommand(params);
            case "trap":
                return executeTrapCommand(params);
            default:
                throw new UnsupportedOperationException("不支持的SNMP命令: " + command);
        }
    }

    /**
     * 执行GET命令
     */
    private Object executeGetCommand(Map<String, Object> params) throws Exception {
        String oid = (String) params.get("oid");
        if (oid == null) {
            throw new IllegalArgumentException("缺少oid参数");
        }

        long startTime = System.currentTimeMillis();

        PDU pdu = new PDU();
        pdu.add(new VariableBinding(new OID(oid)));
        pdu.setType(PDU.GET);

        ResponseEvent response = snmp.send(pdu, target);

        long responseTime = System.currentTimeMillis() - startTime;

        if (response == null || response.getResponse() == null) {
            throw new IOException("GET命令请求超时");
        }

        PDU responsePDU = response.getResponse();
        if (responsePDU.getErrorStatus() != PDU.noError) {
            throw new IOException("GET命令错误: " + responsePDU.getErrorStatusText());
        }

        VariableBinding vb = responsePDU.get(0);
        return Map.of(
                "oid", vb.getOid().toString(),
                "value", convertVariableToJava(vb.getVariable()),
                "type", vb.getVariable().getSyntaxString(),
                "responseTime", responseTime,
                "status", "success"
        );
    }

    /**
     * 执行WALK命令
     */
    private Object executeWalkCommand(Map<String, Object> params) throws Exception {
        String oid = (String) params.get("oid");
        Integer maxRepetitions = (Integer) params.getOrDefault("maxRepetitions", 50);

        if (oid == null) {
            throw new IllegalArgumentException("缺少oid参数");
        }

        List<Map<String, Object>> results = new ArrayList<>();
        OID currentOid = new OID(oid);
        int count = 0;

        while (count < maxRepetitions) {
            PDU pdu = new PDU();
            pdu.add(new VariableBinding(currentOid));
            pdu.setType(PDU.GETNEXT);

            ResponseEvent response = snmp.send(pdu, target);

            if (response == null || response.getResponse() == null) {
                break;
            }

            PDU responsePDU = response.getResponse();
            if (responsePDU.getErrorStatus() != PDU.noError) {
                break;
            }

            VariableBinding vb = responsePDU.get(0);
            OID responseOid = vb.getOid();

            // 检查是否还在子树中
            if (!responseOid.startsWith(currentOid)) {
                break;
            }

            results.add(Map.of(
                    "oid", responseOid.toString(),
                    "value", convertVariableToJava(vb.getVariable()),
                    "type", vb.getVariable().getSyntaxString(),
                    "index", count
            ));

            currentOid = responseOid;
            count++;
        }

        return Map.of(
                "results", results,
                "count", results.size(),
                "status", "success"
        );
    }

    /**
     * 执行GETNEXT命令
     */
    private Object executeGetNextCommand(Map<String, Object> params) throws Exception {
        return executeWalkCommand(params);
    }

    /**
     * 执行GETBULK命令
     */
    private Object executeGetBulkCommand(Map<String, Object> params) throws Exception {
        @SuppressWarnings("unchecked")
        List<String> oids = (List<String>) params.get("oids");
        Integer nonRepeaters = (Integer) params.getOrDefault("nonRepeaters", 0);
        Integer maxRepetitions = (Integer) params.getOrDefault("maxRepetitions", 10);

        if (oids == null || oids.isEmpty()) {
            throw new IllegalArgumentException("缺少oids参数");
        }

        PDU pdu = new PDU();
        for (String oid : oids) {
            pdu.add(new VariableBinding(new OID(oid)));
        }

        // 对于bulk请求，需要使用GETBULK类型（仅v2c和v3支持）
        if (target.getVersion() == SnmpConstants.version2c ||
                target.getVersion() == SnmpConstants.version3) {
            pdu.setType(PDU.GETBULK);
            pdu.setNonRepeaters(nonRepeaters);
            pdu.setMaxRepetitions(maxRepetitions);
        } else {
            pdu.setType(PDU.GETNEXT);
        }

        ResponseEvent response = snmp.send(pdu, target);

        if (response == null || response.getResponse() == null) {
            throw new IOException("GETBULK命令请求超时");
        }

        PDU responsePDU = response.getResponse();
        if (responsePDU.getErrorStatus() != PDU.noError) {
            throw new IOException("GETBULK命令错误: " + responsePDU.getErrorStatusText());
        }

        List<Map<String, Object>> results = new ArrayList<>();
        for (int i = 0; i < responsePDU.size(); i++) {
            VariableBinding vb = responsePDU.get(i);
            results.add(Map.of(
                    "oid", vb.getOid().toString(),
                    "value", convertVariableToJava(vb.getVariable()),
                    "type", vb.getVariable().getSyntaxString(),
                    "index", i
            ));
        }

        return Map.of(
                "results", results,
                "count", results.size(),
                "status", "success"
        );
    }

    /**
     * 执行TRAP命令
     */
    private Object executeTrapCommand(Map<String, Object> params) throws Exception {
        // 发送Trap需要更复杂的配置，这里只是示例
        log.warn("SNMP TRAP命令需要额外的配置，暂不支持");

        return Map.of(
                "status", "not_supported",
                "message", "TRAP命令需要额外的配置"
        );
    }

    /**
     * 处理轮询数据
     */
    private void handlePollingData(String pointId, Object value) {
        // 这里可以处理轮询到的数据，例如：
        // 1. 检查数据变化
        // 2. 触发事件
        // 3. 更新缓存
        // 4. 上报数据
        log.debug("SNMP轮询数据处理: pointId={}, value={}", pointId, value);

        // TODO: 实现具体的轮询数据处理逻辑
    }

    /**
     * 转换SNMP Variable为Java对象
     */
    private Object convertVariableToJava(Variable variable) {
        if (variable == null) {
            return null;
        }

        if (variable instanceof Integer32) {
            return ((Integer32) variable).toInt();
        } else if (variable instanceof Counter32) {
            return ((Counter32) variable).toInt();
        } else if (variable instanceof Gauge32) {
            return ((Gauge32) variable).toInt();
        } else if (variable instanceof TimeTicks) {
            return ((TimeTicks) variable).toInt();
        } else if (variable instanceof Counter64) {
            return ((Counter64) variable).toLong();
        } else if (variable instanceof OctetString) {
            return ((OctetString) variable).toString();
        } else if (variable instanceof IpAddress) {
            return ((IpAddress) variable).toString();
        } else if (variable instanceof OID) {
            return ((OID) variable).toString();
        } else if (variable instanceof Null) {
            return null;
        } else {
            return variable.toString();
        }
    }

    /**
     * 转换Java对象为SNMP Variable
     */
    private Variable convertJavaToVariable(Object value, String dataType) {
        if (value == null) {
            return new Null();
        }

        // 根据数据类型创建相应的Variable
        switch (dataType.toUpperCase()) {
            case "INTEGER":
            case "INT32":
                return new Integer32(((Number) value).intValue());
            case "COUNTER32":
                return new Counter32(((Number) value).intValue());
            case "GAUGE32":
                return new Gauge32(((Number) value).intValue());
            case "TIMETICKS":
                return new TimeTicks(((Number) value).longValue());
            case "COUNTER64":
                return new Counter64(((Number) value).longValue());
            case "OCTETSTRING":
            case "STRING":
                return new OctetString(value.toString());
            case "IPADDRESS":
                return new IpAddress(value.toString());
            case "OID":
                return new OID(value.toString());
            case "NULL":
                return new Null();
            default:
                // 默认尝试转换为Integer32
                try {
                    return new Integer32(Integer.parseInt(value.toString()));
                } catch (NumberFormatException e) {
                    // 如果无法转换为数字，使用OctetString
                    return new OctetString(value.toString());
                }
        }
    }

    /**
     * 估计Variable的大小（字节数）
     */
    private int estimateSize(Variable variable) {
        if (variable == null) {
            return 0;
        }

        if (variable instanceof OctetString) {
            return ((OctetString) variable).length();
        } else if (variable instanceof OID) {
            return ((OID) variable).size() * 4; // 每个子标识符大约4字节
        } else if (variable instanceof Integer32 ||
                variable instanceof Counter32 ||
                variable instanceof Gauge32 ||
                variable instanceof TimeTicks) {
            return 4; // 32位整数
        } else if (variable instanceof Counter64) {
            return 8; // 64位整数
        } else if (variable instanceof IpAddress) {
            return 4; // IPv4地址
        } else {
            return variable.toString().getBytes().length;
        }
    }

    @Override
    public void destroy() {
        try {
            disconnect();
        } catch (Exception e) {
            log.error("SNMP采集器销毁失败", e);
        }

        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        super.destroy();
    }
}