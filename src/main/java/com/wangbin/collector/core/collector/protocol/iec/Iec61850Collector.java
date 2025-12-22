package com.wangbin.collector.core.collector.protocol.iec;

import com.beanit.iec61850bean.BasicDataAttribute;
import com.beanit.iec61850bean.Fc;
import com.beanit.iec61850bean.FcModelNode;
import com.beanit.iec61850bean.ServiceError;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.iec.base.AbstractIec61850Collector;
import com.wangbin.collector.core.collector.protocol.iec.domain.Iec61850Address;
import com.wangbin.collector.core.collector.protocol.iec.util.Iec61850AddressParser;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * IEC61850 采集器实现。
 */
@Slf4j
public class Iec61850Collector extends AbstractIec61850Collector {

    private final Map<String, DataPoint> subscriptionPoints = new ConcurrentHashMap<>();

    @Override
    public String getCollectorType() {
        return "IEC61850";
    }

    @Override
    public String getProtocolType() {
        return "IEC61850";
    }

    @Override
    protected void doConnect() throws Exception {
        connectAssociation();
    }

    @Override
    protected void doDisconnect() {
        closeAssociation();
        log.info("IEC61850连接已断开: {}:{}", host, port);
    }

    @Override
    protected Object doReadPoint(DataPoint point) throws Exception {
        Iec61850Address address = Iec61850AddressParser.parse(point, resolveDefaultFc(point));
        return readAttribute(address);
    }

    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) {
        Map<String, Object> results = new HashMap<>();
        for (DataPoint point : points) {
            try {
                Object value = doReadPoint(point);
                results.put(point.getPointId(), value);
            } catch (Exception e) {
                log.error("IEC61850批量读取失败, point={}", point.getPointName(), e);
                results.put(point.getPointId(), null);
            }
        }
        return results;
    }

    @Override
    protected boolean doWritePoint(DataPoint point, Object value) throws Exception {
        Iec61850Address address = Iec61850AddressParser.parse(point, resolveDefaultFc(point));
        log.debug("IEC61850写入: point={}, address={}, value={}",
                point.getPointName(), address.getObjectReference(), value);
        return writeAttribute(address, value);
    }

    @Override
    protected Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) {
        Map<String, Boolean> results = new HashMap<>();
        for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
            try {
                boolean success = doWritePoint(entry.getKey(), entry.getValue());
                results.put(entry.getKey().getPointId(), success);
            } catch (Exception e) {
                log.error("IEC61850写入失败, point={}", entry.getKey().getPointName(), e);
                results.put(entry.getKey().getPointId(), false);
            }
        }
        return results;
    }

    @Override
    protected void doSubscribe(List<DataPoint> points) {
        for (DataPoint point : points) {
            try {
                Iec61850Address address = Iec61850AddressParser.parse(point, resolveDefaultFc(point));
                subscriptionPoints.put(address.getCacheKey(), point);
            } catch (Exception e) {
                log.warn("IEC61850订阅解析失败, point={}", point.getPointName(), e);
            }
            subscribedPointMap.put(point.getPointId(), point);
        }
        log.info("IEC61850订阅完成, size={}", points.size());
    }

    @Override
    protected void doUnsubscribe(List<DataPoint> points) {
        if (points == null || points.isEmpty()) {
            subscriptionPoints.clear();
            log.info("IEC61850取消所有订阅");
            return;
        }
        for (DataPoint point : points) {
            try {
                Iec61850Address address = Iec61850AddressParser.parse(point, resolveDefaultFc(point));
                subscriptionPoints.remove(address.getCacheKey());
            } catch (Exception e) {
                log.warn("IEC61850取消订阅解析失败, point={}", point.getPointName(), e);
            }
            subscribedPointMap.remove(point.getPointId());
        }
        log.info("IEC61850取消订阅, size={}", points.size());
    }

    @Override
    protected Map<String, Object> doGetDeviceStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("host", host);
        status.put("port", port);
        status.put("connected", association != null);
        status.put("modelLoaded", serverModel != null);
        status.put("timeout", timeout);
        status.put("lastError", lastError);
        return status;
    }

    @Override
    protected Object doExecuteCommand(int unitId, String command, Map<String, Object> params) throws Exception {
        switch (command.toLowerCase()) {
            case "reload_model":
            case "refresh_model":
                reloadServerModel();
                return "model reloaded";
            case "set_timeout":
                Object timeoutValue = params.get("timeout");
                if (timeoutValue == null) {
                    throw new IllegalArgumentException("缺少timeout参数");
                }
                int newTimeout = Integer.parseInt(timeoutValue.toString());
                timeout = newTimeout;
                if (association != null) {
                    association.setResponseTimeout(newTimeout);
                }
                return "timeout updated";
            case "read":
            case "read_raw": {
                Iec61850Address address = parseCommandAddress(params);
                return readAttribute(address);
            }
            case "write":
            case "write_raw": {
                Iec61850Address address = parseCommandAddress(params);
                Object value = params.get("value");
                if (value == null) {
                    throw new IllegalArgumentException("缺少value参数");
                }
                writeAttribute(address, value);
                return "write success";
            }
            case "select": {
                Iec61850Address address = parseCommandAddress(params);
                FcModelNode node = resolveFcNode(address);
                boolean ok = association.select(node);
                return ok;
            }
            case "operate": {
                Iec61850Address address = parseCommandAddress(params);
                FcModelNode node = resolveFcNode(address);
                Object value = params.get("value");
                if (value != null && node instanceof BasicDataAttribute attribute) {
                    applyWriteValue(attribute, value);
                }
                association.operate(node);
                return "operate sent";
            }
            default:
                throw new IllegalArgumentException("Unsupported IEC61850 command: " + command);
        }
    }

    @Override
    protected void buildReadPlans(String deviceId, List<DataPoint> points) {
        log.info("IEC61850加载{}个采集点", points.size());
    }

    private Fc resolveDefaultFc(DataPoint point) {
        String dataType = point.getDataType();
        if (dataType == null) {
            return Fc.ST;
        }
        return switch (dataType.toUpperCase()) {
            case "ANALOG", "FLOAT", "DOUBLE" -> Fc.MX;
            case "CONTROL", "COMMAND" -> Fc.CO;
            default -> Fc.ST;
        };
    }

    @Override
    protected void handleReportValue(BasicDataAttribute attribute, Object value) {
        String key = attribute.getReference() + "@" + attribute.getFc();
        DataPoint point = subscriptionPoints.get(key);
        if (point == null) {
            return;
        }
        Object processed;
        try {
            processed = convertData(point, value);
        } catch (Exception e) {
            processed = value;
            log.debug("IEC61850上送值转换失败, 使用原值: pointId={}", point.getPointId(), e);
        }
        log.info("IEC61850上送: pointId={} value={}", point.getPointId(), processed);
    }

    private Iec61850Address parseCommandAddress(Map<String, Object> params) {
        Object addr = params.get("address");
        if (addr == null) {
            throw new IllegalArgumentException("缺少address参数");
        }
        String fc = params.get("fc") != null ? params.get("fc").toString() : null;
        return Iec61850AddressParser.parse(addr.toString(), fc, Fc.ST);
    }

    private FcModelNode resolveFcNode(Iec61850Address address) {
        if (address == null) {
            throw new IllegalArgumentException("地址解析失败");
        }
        com.beanit.iec61850bean.ModelNode node = findModelNode(address);
        if (!(node instanceof FcModelNode fcNode)) {
            throw new IllegalArgumentException("地址不是功能约束节点: " + address);
        }
        return fcNode;
    }
}
