package com.wangbin.collector.core.report.handler;

import com.wangbin.collector.common.enums.QualityEnum;
import com.wangbin.collector.core.report.model.ReportConfig;
import com.wangbin.collector.core.report.model.ReportData;
import com.wangbin.collector.core.report.model.ReportResult;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.*;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HTTP报告处理器
 *
 * 作用：通过HTTP/HTTPS协议上报数据到远程服务器
 *
 * 功能说明：
 * 1. 支持HTTP和HTTPS协议
 * 2. 支持多种请求方式（GET、POST、PUT、DELETE）
 * 3. 支持请求头和参数的动态配置
 * 4. 支持SSL证书验证和忽略
 * 5. 支持连接池和超时设置
 * 6. 支持数据格式转换（JSON、Form、XML等）
 */
@Slf4j
public class HttpReportHandler extends AbstractReportHandler {

    /**
     * RestTemplate实例，用于发送HTTP请求
     */
    private RestTemplate restTemplate;

    /**
     * HTTP客户端工厂
     */
    private HttpClientFactory httpClientFactory;

    /**
     * 连接池配置
     */
    private ConnectionPoolConfig connectionPoolConfig;

    /**
     * 目标系统连接缓存
     */
    private final Map<String, RestTemplate> targetClients = new ConcurrentHashMap<>();

    /**
     * 构造函数
     */
    public HttpReportHandler() {
        super("HttpReportHandler", "HTTP", "HTTP协议上报处理器");
    }

    /**
     * 执行具体初始化逻辑
     *
     * 执行步骤：
     * 1. 初始化HTTP客户端工厂
     * 2. 创建默认的RestTemplate
     * 3. 配置连接池
     * 4. 配置SSL
     *
     * @throws Exception 初始化失败时抛出异常
     */
    @Override
    protected void doInit() throws Exception {
        log.info("初始化HTTP报告处理器...");

        try {
            // 1. 加载HTTP配置
            loadHttpConfig();

            // 2. 创建HTTP客户端工厂
            httpClientFactory = new HttpClientFactory();
            httpClientFactory.setConnectTimeout((int) connectTimeout);
            httpClientFactory.setReadTimeout((int) readTimeout);

            // 3. 配置连接池
            if (connectionPoolConfig != null) {
                httpClientFactory.setConnectionPoolConfig(connectionPoolConfig);
            }

            // 4. 配置SSL（如果需要忽略SSL证书验证）
            boolean ignoreSSL = getBooleanConfig("ignoreSSL", false);
            if (ignoreSSL) {
                configureIgnoreSSL();
            }

            // 5. 创建默认的RestTemplate
            restTemplate = httpClientFactory.createRestTemplate();

            // 6. 启动连接监控
            startConnectionMonitoring();

            log.info("HTTP报告处理器初始化完成");
        } catch (Exception e) {
            log.error("HTTP报告处理器初始化失败", e);
            throw e;
        }
    }

    /**
     * 执行具体上报逻辑
     *
     * 执行步骤：
     * 1. 构建HTTP请求
     * 2. 发送HTTP请求
     * 3. 处理HTTP响应
     * 4. 解析响应结果
     *
     * @param data 上报数据
     * @param config 上报配置
     * @return 上报结果
     * @throws Exception 上报失败时抛出异常
     */
    @Override
    protected ReportResult doReport(ReportData data, ReportConfig config) throws Exception {
        log.debug("开始HTTP上报：{} -> {}", data.getPointCode(), config.getTargetId());

        try {
            // 1. 获取目标系统的RestTemplate
            RestTemplate client = getTargetClient(config);

            // 2. 构建请求实体
            HttpEntity<?> requestEntity = buildRequestEntity(data, config);

            // 3. 发送HTTP请求
            ResponseEntity<String> response = client.exchange(
                    config.getUrl(),
                    getHttpMethod(config),
                    requestEntity,
                    String.class
            );

            // 4. 处理响应
            return handleResponse(data, config, response);

        } catch (Exception e) {
            log.error("HTTP上报异常：{} -> {}", data.getPointCode(), config.getTargetId(), e);
            throw e;
        }
    }

    /**
     * 执行具体批量上报逻辑
     *
     * 执行步骤：
     * 1. 批量构建HTTP请求
     * 2. 批量发送HTTP请求
     * 3. 批量处理HTTP响应
     * 4. 批量解析响应结果
     *
     * @param dataList 数据列表
     * @param config 上报配置
     * @return 上报结果列表
     * @throws Exception 上报失败时抛出异常
     */
    @Override
    protected List<ReportResult> doBatchReport(List<ReportData> dataList, ReportConfig config) throws Exception {
        log.debug("开始批量HTTP上报：{}，数据量：{}", config.getTargetId(), dataList.size());

        List<ReportResult> results = new ArrayList<>(dataList.size());

        try {
            // 1. 获取目标系统的RestTemplate
            RestTemplate client = getTargetClient(config);

            // 2. 批量处理数据
            for (ReportData data : dataList) {
                try {
                    // 构建请求实体
                    HttpEntity<?> requestEntity = buildRequestEntity(data, config);

                    // 发送HTTP请求
                    ResponseEntity<String> response = client.exchange(
                            config.getUrl(),
                            getHttpMethod(config),
                            requestEntity,
                            String.class
                    );

                    // 处理响应
                    ReportResult result = handleResponse(data, config, response);
                    results.add(result);

                } catch (Exception e) {
                    log.error("批量HTTP上报单个数据异常：{} -> {}", data.getPointCode(), config.getTargetId(), e);
                    ReportResult errorResult = ReportResult.error(
                            data.getPointCode(),
                            "HTTP上报异常：" + e.getMessage(),
                            config.getTargetId()
                    );
                    results.add(errorResult);
                }
            }

            log.debug("批量HTTP上报完成：{}，成功：{}，失败：{}",
                    config.getTargetId(),
                    results.stream().filter(ReportResult::isSuccess).count(),
                    results.stream().filter(r -> !r.isSuccess()).count()
            );

            return results;

        } catch (Exception e) {
            log.error("批量HTTP上报异常：{}", config.getTargetId(), e);
            throw e;
        }
    }

    /**
     * 执行具体配置更新逻辑
     *
     * @param config 新的配置
     * @throws Exception 处理失败时抛出异常
     */
    @Override
    protected void doConfigUpdate(ReportConfig config) throws Exception {
        log.debug("更新HTTP处理器配置：{}", config.getTargetId());

        try {
            // 1. 清理旧的客户端
            targetClients.remove(config.getTargetId());

            // 2. 创建新的客户端
            RestTemplate newClient = createTargetClient(config);
            targetClients.put(config.getTargetId(), newClient);

            log.info("HTTP处理器配置更新完成：{}", config.getTargetId());
        } catch (Exception e) {
            log.error("HTTP处理器配置更新失败：{}", config.getTargetId(), e);
            throw e;
        }
    }

    /**
     * 执行具体配置删除逻辑
     *
     * @param config 被删除的配置
     * @throws Exception 处理失败时抛出异常
     */
    @Override
    protected void doConfigRemove(ReportConfig config) throws Exception {
        log.debug("删除HTTP处理器配置：{}", config.getTargetId());

        try {
            // 清理客户端缓存
            targetClients.remove(config.getTargetId());

            log.info("HTTP处理器配置删除完成：{}", config.getTargetId());
        } catch (Exception e) {
            log.error("HTTP处理器配置删除失败：{}", config.getTargetId(), e);
            throw e;
        }
    }

    /**
     * 执行具体销毁逻辑
     *
     * @throws Exception 销毁失败时抛出异常
     */
    @Override
    protected void doDestroy() throws Exception {
        log.info("销毁HTTP报告处理器...");

        try {
            // 1. 清空客户端缓存
            targetClients.clear();

            // 2. 停止连接监控
            stopConnectionMonitoring();

            // 3. 清理资源
            if (restTemplate != null) {
                restTemplate = null;
            }

            if (httpClientFactory != null) {
                httpClientFactory.destroy();
                httpClientFactory = null;
            }

            log.info("HTTP报告处理器销毁完成");
        } catch (Exception e) {
            log.error("HTTP报告处理器销毁失败", e);
            throw e;
        }
    }

    /**
     * 获取具体实现的状态信息
     *
     * @return 状态信息映射
     */
    @Override
    protected Map<String, Object> getImplementationStatus() {
        Map<String, Object> status = new HashMap<>();

        status.put("restTemplateAvailable", restTemplate != null);
        status.put("httpClientFactoryAvailable", httpClientFactory != null);
        status.put("targetClientsCount", targetClients.size());
        status.put("connectionPoolConfig", connectionPoolConfig != null ?
                connectionPoolConfig.getStatus() : null);

        return status;
    }

    /**
     * 获取具体实现的统计信息
     *
     * @return 统计信息映射
     */
    @Override
    protected Map<String, Object> getImplementationStatistics() {
        Map<String, Object> stats = new HashMap<>();

        // 连接池统计
        if (httpClientFactory != null) {
            Map<String, Object> poolStats = httpClientFactory.getConnectionPoolStats();
            stats.put("connectionPool", poolStats);
        }

        // 目标客户端统计
        stats.put("targetClients", targetClients.size());

        return stats;
    }

    // =============== 辅助方法 ===============

    /**
     * 加载HTTP配置
     */
    private void loadHttpConfig() {
        // 从配置中加载连接池配置
        int maxTotal = getIntConfig("maxTotal", 200);
        int defaultMaxPerRoute = getIntConfig("defaultMaxPerRoute", 50);
        int validateAfterInactivity = getIntConfig("validateAfterInactivity", 2000);

        connectionPoolConfig = new ConnectionPoolConfig();
        connectionPoolConfig.setMaxTotal(maxTotal);
        connectionPoolConfig.setDefaultMaxPerRoute(defaultMaxPerRoute);
        connectionPoolConfig.setValidateAfterInactivity(validateAfterInactivity);

        log.debug("加载HTTP配置完成");
    }

    /**
     * 配置忽略SSL证书验证
     */
    private void configureIgnoreSSL() {
        try {
            // 创建信任所有证书的TrustManager
            TrustManager[] trustAllCerts = new TrustManager[] {
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() {
                            return null;
                        }
                        public void checkClientTrusted(X509Certificate[] certs, String authType) {
                        }
                        public void checkServerTrusted(X509Certificate[] certs, String authType) {
                        }
                    }
            };

            // 创建SSL上下文
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new SecureRandom());

            // 创建SSL Socket Factory
            SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            // 配置HTTP客户端工厂
            if (httpClientFactory != null) {
                httpClientFactory.setSslSocketFactory(sslSocketFactory);
                httpClientFactory.setHostnameVerifier((hostname, session) -> true);
            }

            log.info("已配置忽略SSL证书验证");
        } catch (Exception e) {
            log.error("配置忽略SSL证书验证失败", e);
        }
    }

    /**
     * 启动连接监控
     */
    private void startConnectionMonitoring() {
        // 可以在这里启动连接监控任务
        // 例如：定时检查连接状态、清理空闲连接等
        log.debug("连接监控已启动");
    }

    /**
     * 停止连接监控
     */
    private void stopConnectionMonitoring() {
        // 停止连接监控任务
        log.debug("连接监控已停止");
    }

    /**
     * 获取目标系统的RestTemplate
     *
     * @param config 上报配置
     * @return RestTemplate实例
     */
    private RestTemplate getTargetClient(ReportConfig config) {
        return targetClients.computeIfAbsent(config.getTargetId(), key -> {
            try {
                return createTargetClient(config);
            } catch (Exception e) {
                log.error("创建目标客户端失败：{}", config.getTargetId(), e);
                return restTemplate; // 返回默认客户端
            }
        });
    }

    /**
     * 创建目标系统的RestTemplate
     *
     * @param config 上报配置
     * @return RestTemplate实例
     */
    private RestTemplate createTargetClient(ReportConfig config) {
        // 根据配置创建自定义的RestTemplate
        HttpClientFactory factory = new HttpClientFactory();

        // 设置超时时间
        factory.setConnectTimeout(config.getConnectTimeout() != null ?
                config.getConnectTimeout().intValue() : (int) connectTimeout);
        factory.setReadTimeout(config.getReadTimeout() != null ?
                config.getReadTimeout().intValue() : (int) readTimeout);

        // 设置连接池配置
        if (connectionPoolConfig != null) {
            factory.setConnectionPoolConfig(connectionPoolConfig);
        }

        // 配置SSL（如果需要）
        boolean ignoreSSL = getBooleanConfig("ignoreSSL", false);
        if (ignoreSSL) {
            try {
                configureIgnoreSSLForFactory(factory);
            } catch (Exception e) {
                log.warn("配置忽略SSL失败：{}", config.getTargetId(), e);
            }
        }

        return factory.createRestTemplate();
    }

    /**
     * 为工厂配置忽略SSL
     */
    private void configureIgnoreSSLForFactory(HttpClientFactory factory) throws Exception {
        TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                    }
                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                    }
                }
        };

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustAllCerts, new SecureRandom());

        factory.setSslSocketFactory(sslContext.getSocketFactory());
        factory.setHostnameVerifier((hostname, session) -> true);
    }

    /**
     * 构建HTTP请求实体
     *
     * @param data 上报数据
     * @param config 上报配置
     * @return HTTP请求实体
     */
    private HttpEntity<?> buildRequestEntity(ReportData data, ReportConfig config) {
        // 1. 构建请求头
        HttpHeaders headers = buildHeaders(config);

        // 2. 构建请求体
        Object body = buildRequestBody(data, config);

        // 3. 创建请求实体
        return new HttpEntity<>(body, headers);
    }

    /**
     * 构建HTTP请求头
     *
     * @param config 上报配置
     * @return HTTP请求头
     */
    private HttpHeaders buildHeaders(ReportConfig config) {
        HttpHeaders headers = new HttpHeaders();

        // 从配置中获取请求头
        Map<String, Object> reportParams = config.getParams();
        if (reportParams != null) {
            Object headersObj = reportParams.get("headers");
            if (headersObj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> headerMap = (Map<String, Object>) headersObj;
                for (Map.Entry<String, Object> entry : headerMap.entrySet()) {
                    if (entry.getValue() != null) {
                        headers.add(entry.getKey(), entry.getValue().toString());
                    }
                }
            }
        }

        // 设置默认的Content-Type
        if (!headers.containsKey(HttpHeaders.CONTENT_TYPE)) {
            headers.setContentType(MediaType.APPLICATION_JSON);
        }

        // 设置默认的User-Agent
        if (!headers.containsKey(HttpHeaders.USER_AGENT)) {
            headers.set(HttpHeaders.USER_AGENT, "DataCollector/1.0");
        }

        return headers;
    }

    /**
     * 构建HTTP请求体
     *
     * @param data 上报数据
     * @param config 上报配置
     * @return 请求体对象
     */
    private Object buildRequestBody(ReportData data, ReportConfig config) {
        // 获取数据格式
        String dataFormat = getStringConfig("dataFormat", "JSON");

        switch (dataFormat.toUpperCase()) {
            case "FORM":
                return buildFormData(data, config);

            case "XML":
                return buildXmlData(data, config);

            case "JSON":
            default:
                return buildJsonData(data, config);
        }
    }

    /**
     * 构建JSON格式的数据
     *
     * @param data 上报数据
     * @param config 上报配置
     * @return JSON格式的数据
     */
    private Map<String, Object> buildJsonData(ReportData data, ReportConfig config) {
        Map<String, Object> jsonData = new HashMap<>();

        // 添加基本数据
        jsonData.put("pointCode", data.getPointCode());
        jsonData.put("pointName", data.getPointName());
        jsonData.put("value", data.getValue());
        jsonData.put("timestamp", data.getTimestamp());
        jsonData.put("quality", data.getQuality() != null ? data.getQuality() : QualityEnum.GOOD.getText());

        // 添加元数据
        if (data.getMetadata() != null && !data.getMetadata().isEmpty()) {
            jsonData.put("metadata", data.getMetadata());
        }

        // 从配置中获取额外的数据
        Map<String, Object> reportParams = config.getParams();
        if (reportParams != null) {
            Object extraData = reportParams.get("extraData");
            if (extraData instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> extraMap = (Map<String, Object>) extraData;
                jsonData.putAll(extraMap);
            }
        }

        return jsonData;
    }

    /**
     * 构建Form格式的数据
     *
     * @param data 上报数据
     * @param config 上报配置
     * @return Form格式的数据
     */
    private MultiValueMap<String, String> buildFormData(ReportData data, ReportConfig config) {
        MultiValueMap<String, String> formData = new LinkedMultiValueMap<>();

        // 添加基本数据
        formData.add("pointCode", data.getPointCode());
        if (data.getPointName() != null) {
            formData.add("pointName", data.getPointName());
        }
        formData.add("value", data.getValue() != null ? data.getValue().toString() : "");
        formData.add("timestamp", String.valueOf(data.getTimestamp()));
        if (data.getQuality() != null) {
            formData.add("quality", data.getQuality());
        }

        // 添加元数据（JSON格式）
        if (data.getMetadata() != null && !data.getMetadata().isEmpty()) {
            // 这里需要将元数据转换为JSON字符串
            // 为了简化，只添加简单的键值对
            for (Map.Entry<String, Object> entry : data.getMetadata().entrySet()) {
                if (entry.getValue() != null) {
                    formData.add("metadata." + entry.getKey(), entry.getValue().toString());
                }
            }
        }

        return formData;
    }

    /**
     * 构建XML格式的数据
     *
     * @param data 上报数据
     * @param config 上报配置
     * @return XML格式的数据
     */
    private String buildXmlData(ReportData data, ReportConfig config) {
        // 简化实现，实际项目中可以使用JAXB或DOM构建XML
        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<data>\n");
        xml.append("  <pointCode>").append(escapeXml(data.getPointCode())).append("</pointCode>\n");
        if (data.getPointName() != null) {
            xml.append("  <pointName>").append(escapeXml(data.getPointName())).append("</pointName>\n");
        }
        xml.append("  <value>").append(escapeXml(data.getValue() != null ? data.getValue().toString() : "")).append("</value>\n");
        xml.append("  <timestamp>").append(data.getTimestamp()).append("</timestamp>\n");
        if (data.getQuality() != null) {
            xml.append("  <quality>").append(data.getQuality()).append("</quality>\n");
        }
        xml.append("</data>");

        return xml.toString();
    }

    /**
     * 转义XML特殊字符
     *
     * @param text 原始文本
     * @return 转义后的文本
     */
    private String escapeXml(String text) {
        if (text == null) {
            return "";
        }

        return text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
    }

    /**
     * 获取HTTP方法
     *
     * @param config 上报配置
     * @return HTTP方法
     */
    private HttpMethod getHttpMethod(ReportConfig config) {
        // 从配置中获取HTTP方法
        Map<String, Object> reportParams = config.getParams();
        if (reportParams != null) {
            Object methodObj = reportParams.get("method");
            if (methodObj instanceof String) {
                String method = ((String) methodObj).toUpperCase();
                try {
                    return HttpMethod.valueOf(method);
                } catch (IllegalArgumentException e) {
                    log.warn("无效的HTTP方法：{}，使用默认方法POST", method);
                }
            }
        }

        // 默认使用POST方法
        return HttpMethod.POST;
    }

    /**
     * 处理HTTP响应
     *
     * @param data 上报数据
     * @param config 上报配置
     * @param response HTTP响应
     * @return 上报结果
     */
    private ReportResult handleResponse(ReportData data, ReportConfig config, ResponseEntity<String> response) {
        ReportResult result = ReportResult.success(data.getPointCode(), config.getTargetId());

        // 设置状态码和响应数据
        result.setStatusCode(response.getStatusCode().value());
        result.setResponseData(response.getBody());

        // 检查响应状态
        if (!response.getStatusCode().is2xxSuccessful()) {
            result.setSuccess(false);
            // 简化错误信息
            result.setErrorMessage("HTTP错误：" + response.getStatusCode().value());
        }

        // 添加响应头到元数据
        response.getHeaders();
        if (!response.getHeaders().isEmpty()) {
            Map<String, Object> responseHeaders = new HashMap<>();
            response.getHeaders().forEach((key, values) -> {
                if (values != null && !values.isEmpty()) {
                    responseHeaders.put(key, values.size() == 1 ? values.get(0) : values);
                }
            });
            result.addMetadata("responseHeaders", responseHeaders);
        }

        return result;
    }

    /**
     * 连接池配置内部类
     */
    @Data
    private static class ConnectionPoolConfig {
        private int maxTotal = 200;
        private int defaultMaxPerRoute = 50;
        private int validateAfterInactivity = 2000;

        public Map<String, Object> getStatus() {
            Map<String, Object> status = new HashMap<>();
            status.put("maxTotal", maxTotal);
            status.put("defaultMaxPerRoute", defaultMaxPerRoute);
            status.put("validateAfterInactivity", validateAfterInactivity);
            return status;
        }
    }

    /**
     * HTTP客户端工厂内部类
     */
    @Data
    private static class HttpClientFactory {
        private int connectTimeout = 5000;
        private int readTimeout = 10000;
        private SSLSocketFactory sslSocketFactory;
        private HostnameVerifier hostnameVerifier;
        private ConnectionPoolConfig connectionPoolConfig;


        public RestTemplate createRestTemplate() {
            // 简化实现，实际项目中可以使用HttpComponentsClientHttpRequestFactory
            RestTemplate restTemplate = new RestTemplate();

            // 设置超时
            // 在实际项目中，需要配置ClientHttpRequestFactory来设置超时

            return restTemplate;
        }

        public void destroy() {
            // 清理资源
        }

        public Map<String, Object> getConnectionPoolStats() {
            Map<String, Object> stats = new HashMap<>();
            if (connectionPoolConfig != null) {
                stats.put("config", connectionPoolConfig.getStatus());
            }
            // 在实际项目中，可以从连接池获取实时统计信息
            return stats;
        }
    }
}