package com.wangbin.collector.core.report.handler;

import com.wangbin.collector.common.constant.MessageConstant;
import com.wangbin.collector.common.constant.ProtocolConstant;
import com.wangbin.collector.core.report.adapter.BinaryProtocolAdapter;
import com.wangbin.collector.core.report.adapter.IoTProtocolAdapter;
import com.wangbin.collector.core.report.adapter.JsonProtocolAdapter;
import com.wangbin.collector.core.report.model.ReportConfig;
import com.wangbin.collector.core.report.model.ReportData;
import com.wangbin.collector.core.report.model.ReportResult;
import com.wangbin.collector.core.report.model.message.IoTMessage;
import com.wangbin.collector.core.report.service.IoTProtocolService;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

/**
 * TCP上报处理器 - 支持物联网协议
 */
@Slf4j
public class TcpReportHandler extends AbstractReportHandler {

    private final Map<String, Socket> connectionPool = new ConcurrentHashMap<>();
    private final IoTProtocolService protocolService;
    private ScheduledExecutorService heartbeatExecutor;

    public TcpReportHandler(IoTProtocolService protocolService) {
        super("TcpReportHandler", ProtocolConstant.PROTOCOL_TCP, "TCP协议上报处理器（支持物联网协议）");
        this.protocolService = protocolService;
    }

    @Override
    protected void doInit() throws Exception {
        log.info("初始化TCP上报处理器...");
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        heartbeatExecutor.scheduleAtFixedRate(this::checkConnections,
                30, 30, TimeUnit.SECONDS);
    }

    @Override
    protected ReportResult doReport(ReportData data, ReportConfig config) throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            // 验证配置
            if (!ProtocolConstant.PROTOCOL_TCP.equalsIgnoreCase(config.getProtocol())) {
                return ReportResult.error(data.getPointCode(),
                        "协议类型不匹配，期望TCP，实际" + config.getProtocol(), config.getTargetId());
            }

            // 获取TCP连接
            Socket socket = getConnection(config);

            // 获取认证信息
            Map<String, Object> authInfo = getAuthInfo(config);

            // 获取消息类型
            String messageType = getMessageType(config);

            // 转换为物联网消息
            IoTMessage message = protocolService.convertToIoTMessage(data, authInfo, messageType);

            if (message == null) {
                return ReportResult.error(data.getPointCode(),
                        "转换物联网消息失败", config.getTargetId());
            }

            // 根据配置选择协议格式
            byte[] requestData;
            String format = getProtocolFormat(config);

            if ("binary".equalsIgnoreCase(format)) {
                // 二进制格式
                requestData = protocolService.encodeToBinary(message);
            } else {
                // JSON格式（默认）
                String json = protocolService.encodeToJson(message);
                requestData = json != null ? json.getBytes("UTF-8") : null;
            }

            if (requestData == null) {
                return ReportResult.error(data.getPointCode(),
                        "编码消息失败", config.getTargetId());
            }

            // 发送数据
            OutputStream out = socket.getOutputStream();
            out.write(requestData);
            out.flush();

            // 读取响应
            ReportResult result = handleResponse(socket, data, config, startTime);

            log.debug("TCP上报完成: {} -> {}，耗时: {}ms",
                    data.getPointCode(), config.getTargetId(), result.getCostTime());

            return result;

        } catch (Exception e) {
            log.error("TCP上报失败: {} -> {}", data.getPointCode(), config.getTargetId(), e);
            return ReportResult.error(data.getPointCode(),
                    "TCP上报异常: " + e.getMessage(), config.getTargetId());
        }
    }

    @Override
    protected List<ReportResult> doBatchReport(List<ReportData> dataList, ReportConfig config) throws Exception {
        List<ReportResult> results = new ArrayList<>(dataList.size());
        long startTime = System.currentTimeMillis();

        try {
            // 验证配置
            if (!ProtocolConstant.PROTOCOL_TCP.equalsIgnoreCase(config.getProtocol())) {
                for (ReportData data : dataList) {
                    results.add(ReportResult.error(data.getPointCode(),
                            "协议类型不匹配", config.getTargetId()));
                }
                return results;
            }

            // 获取TCP连接
            Socket socket = getConnection(config);

            // 获取认证信息
            Map<String, Object> authInfo = getAuthInfo(config);

            // 获取消息类型
            String messageType = getMessageType(config);

            // 批量处理
            for (ReportData data : dataList) {
                try {
                    // 转换为物联网消息
                    IoTMessage message = protocolService.convertToIoTMessage(data, authInfo, messageType);

                    if (message == null) {
                        results.add(ReportResult.error(data.getPointCode(),
                                "转换消息失败", config.getTargetId()));
                        continue;
                    }

                    // 编码消息
                    byte[] requestData;
                    String format = getProtocolFormat(config);

                    if ("binary".equalsIgnoreCase(format)) {
                        requestData = protocolService.encodeToBinary(message);
                    } else {
                        String json = protocolService.encodeToJson(message);
                        requestData = json != null ? json.getBytes("UTF-8") : null;
                    }

                    if (requestData == null) {
                        results.add(ReportResult.error(data.getPointCode(),
                                "编码消息失败", config.getTargetId()));
                        continue;
                    }

                    // 发送数据
                    OutputStream out = socket.getOutputStream();
                    out.write(requestData);
                    out.flush();

                    // 处理响应
                    ReportResult result = handleResponse(socket, data, config, startTime);
                    results.add(result);

                } catch (Exception e) {
                    log.error("批量上报单个数据失败: {}", data.getPointCode(), e);
                    results.add(ReportResult.error(data.getPointCode(),
                            e.getMessage(), config.getTargetId()));
                }
            }

            long totalTime = System.currentTimeMillis() - startTime;
            long successCount = results.stream().filter(ReportResult::isSuccess).count();

            log.info("批量TCP上报完成: {}，成功: {}，失败: {}，总计: {}，总耗时: {}ms",
                    config.getTargetId(), successCount, results.size() - successCount,
                    results.size(), totalTime);

            return results;

        } catch (Exception e) {
            log.error("批量TCP上报异常: {}", config.getTargetId(), e);
            for (ReportData data : dataList) {
                results.add(ReportResult.error(data.getPointCode(),
                        "批量上报异常", config.getTargetId()));
            }
            return results;
        }
    }

    private Socket getConnection(ReportConfig config) throws Exception {
        String key = getConnectionKey(config);

        return connectionPool.computeIfAbsent(key, k -> {
            try {
                // 创建新连接
                Socket socket = new Socket(config.getHost(), config.getPort());
                socket.setKeepAlive(true);
                socket.setSoTimeout(getReadTimeout());

                log.info("创建TCP连接: {}:{}", config.getHost(), config.getPort());

                // 如果需要认证，执行认证
                if (needsAuthentication(config)) {
                    performAuthentication(socket, config);
                }

                return socket;

            } catch (Exception e) {
                log.error("创建TCP连接失败: {}:{}", config.getHost(), config.getPort(), e);
                throw new RuntimeException("创建TCP连接失败", e);
            }
        });
    }

    private String getConnectionKey(ReportConfig config) {
        return config.getTargetId() + "@" + config.getHost() + ":" + config.getPort();
    }

    private Map<String, Object> getAuthInfo(ReportConfig config) {
        Map<String, Object> authInfo = new HashMap<>();

        if (config.getParams() != null) {
            authInfo.putAll(config.getParams());
        }

        // 设置默认的clientId和username
        String productKey = (String) authInfo.get(MessageConstant.FIELD_PRODUCT_KEY);
        String deviceName = (String) authInfo.get(MessageConstant.FIELD_DEVICE_NAME);

        if (productKey != null && deviceName != null) {
            authInfo.put(MessageConstant.FIELD_CLIENT_ID, productKey + "." + deviceName);
            authInfo.put(MessageConstant.FIELD_USERNAME, deviceName + "&" + productKey);
        }

        return authInfo;
    }

    private boolean needsAuthentication(ReportConfig config) {
        Map<String, Object> authParams = config.getParams();
        return authParams != null &&
                authParams.containsKey(MessageConstant.FIELD_PRODUCT_KEY) &&
                authParams.containsKey(MessageConstant.FIELD_DEVICE_NAME);
    }

    private void performAuthentication(Socket socket, ReportConfig config) throws Exception {
        Map<String, Object> authParams = config.getParams();
        String productKey = (String) authParams.get(MessageConstant.FIELD_PRODUCT_KEY);
        String deviceName = (String) authParams.get(MessageConstant.FIELD_DEVICE_NAME);
        String password = (String) authParams.get(MessageConstant.FIELD_PASSWORD);

        if (productKey == null || deviceName == null) {
            log.warn("认证信息不完整，跳过认证");
            return;
        }

        // 创建认证消息
        IoTMessage authMessage = protocolService.createAuthMessage(productKey, deviceName, password);

        // 发送认证
        OutputStream out = socket.getOutputStream();
        String format = getProtocolFormat(config);

        byte[] authData;
        if ("binary".equalsIgnoreCase(format)) {
            authData = protocolService.encodeToBinary(authMessage);
        } else {
            String json = protocolService.encodeToJson(authMessage);
            authData = json != null ? json.getBytes("UTF-8") : null;
        }

        if (authData != null) {
            out.write(authData);
            out.flush();

            // 读取认证响应
            InputStream in = socket.getInputStream();
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);

            if (bytesRead > 0) {
                String response = new String(buffer, 0, bytesRead, "UTF-8");
                log.debug("TCP认证响应: {}", response);

                // 可以解析响应判断认证是否成功
                if (response.contains("success") || response.contains("200")) {
                    log.info("TCP认证成功: {} -> {}:{}",
                            deviceName, config.getHost(), config.getPort());
                } else {
                    log.warn("TCP认证失败: {}", response);
                }
            }
        }
    }

    private String getMessageType(ReportConfig config) {
        if (config.getParams() != null) {
            Object messageType = config.getParams().get("messageType");
            if (messageType instanceof String) {
                return (String) messageType;
            }
        }
        return MessageConstant.MESSAGE_TYPE_PROPERTY_POST;
    }

    private String getProtocolFormat(ReportConfig config) {
        if (config.getParams() != null) {
            Object format = config.getParams().get("format");
            if (format instanceof String) {
                return ((String) format).toLowerCase();
            }
        }
        return "json"; // 默认JSON格式
    }

    private ReportResult handleResponse(Socket socket, ReportData data,
                                        ReportConfig config, long startTime) throws Exception {
        ReportResult result = ReportResult.success(data.getPointCode(), config.getTargetId());
        result.setCostTime(System.currentTimeMillis() - startTime);

        try {
            InputStream in = socket.getInputStream();
            if (in.available() > 0) {
                byte[] buffer = new byte[1024];
                int bytesRead = in.read(buffer);

                if (bytesRead > 0) {
                    String response = new String(buffer, 0, bytesRead, "UTF-8");
                    result.setResponseData(response);

                    // 解析响应状态
                    if (response.contains("\"code\":0") || response.contains("success")) {
                        result.setSuccess(true);
                    } else {
                        result.setSuccess(false);
                        result.setErrorMessage("服务器返回错误: " + response);
                    }
                }
            }
        } catch (Exception e) {
            log.debug("读取响应失败: {}", data.getPointCode(), e);
            // 读取响应失败不影响上报成功
        }

        return result;
    }

    private void checkConnections() {
        Iterator<Map.Entry<String, Socket>> iterator = connectionPool.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Socket> entry = iterator.next();
            Socket socket = entry.getValue();

            if (socket == null || socket.isClosed() || !socket.isConnected()) {
                try {
                    if (socket != null) {
                        socket.close();
                    }
                } catch (Exception e) {
                    log.debug("关闭无效连接失败", e);
                }
                iterator.remove();
                log.debug("清理无效TCP连接: {}", entry.getKey());
            }
        }
    }

    @Override
    protected void doConfigUpdate(ReportConfig config) throws Exception {
        // 清理旧的连接
        String key = getConnectionKey(config);
        Socket oldSocket = connectionPool.remove(key);
        if (oldSocket != null) {
            try {
                oldSocket.close();
            } catch (Exception e) {
                log.debug("关闭旧连接失败", e);
            }
        }
        log.info("TCP配置已更新: {}", config.getTargetId());
    }

    @Override
    protected void doConfigRemove(ReportConfig config) throws Exception {
        doConfigUpdate(config);
    }

    @Override
    protected void doDestroy() throws Exception {
        // 关闭所有连接
        for (Socket socket : connectionPool.values()) {
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (Exception e) {
                log.debug("关闭TCP连接失败", e);
            }
        }
        connectionPool.clear();

        // 关闭心跳任务
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
            try {
                if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    heartbeatExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                heartbeatExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    protected Map<String, Object> getImplementationStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("activeConnections", connectionPool.size());
        status.put("heartbeatRunning", heartbeatExecutor != null && !heartbeatExecutor.isShutdown());
        return status;
    }

    @Override
    protected Map<String, Object> getImplementationStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("connectionCount", connectionPool.size());
        return stats;
    }
}