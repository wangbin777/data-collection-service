package com.wangbin.collector.core.connection.adapter;

import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.common.exception.CollectorException;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.DiscoveryClient;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.OpcUaClientConfigBuilder;
import org.eclipse.milo.opcua.sdk.client.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.client.identity.UsernameProvider;
import org.eclipse.milo.opcua.sdk.client.identity.X509IdentityProvider;
import org.eclipse.milo.opcua.stack.core.security.CertificateQuarantine;
import org.eclipse.milo.opcua.stack.core.security.CertificateValidator;
import org.eclipse.milo.opcua.stack.core.security.DefaultClientCertificateValidator;
import org.eclipse.milo.opcua.stack.core.security.FileBasedCertificateQuarantine;
import org.eclipse.milo.opcua.stack.core.security.FileBasedTrustListManager;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.security.TrustListManager;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.enumerated.UserTokenType;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.UserTokenPolicy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * OPC UA connection adapter backed by Milo {@link OpcUaClient}.
 */
@Slf4j
public class OpcUaConnectionAdapter extends AbstractConnectionAdapter<OpcUaClient> {

    private static final String OPC_POLICY_URI_PREFIX = "http://opcfoundation.org/UA/SecurityPolicy#";
    private static final Pattern CERT_PATTERN =
            Pattern.compile("-----BEGIN CERTIFICATE-----(.*?)-----END CERTIFICATE-----", Pattern.DOTALL);
    private static final Pattern PRIVATE_KEY_PATTERN =
            Pattern.compile("-----BEGIN PRIVATE KEY-----(.*?)-----END PRIVATE KEY-----", Pattern.DOTALL);
    private static final long MIN_TIMEOUT_MS = 5000L;

    private OpcUaClient client;
    private String endpointUrl;
    private String securityPolicy;
    private MessageSecurityMode securityMode;

    public OpcUaConnectionAdapter(DeviceInfo deviceInfo, DeviceConnection config) {
        super(deviceInfo, config);
    }

    @Override
    protected void doConnect() throws Exception {
        this.endpointUrl = resolveEndpointUrl();
        this.securityPolicy = resolveSecurityPolicy();
        this.securityMode = resolveSecurityMode();

        List<EndpointDescription> endpoints = discoverEndpoints(endpointUrl);
        EndpointDescription endpoint = selectEndpoint(endpoints);

        ClientKeyMaterial clientKeyMaterial = loadClientKeyMaterial();
        OpcUaClientConfigBuilder builder = new OpcUaClientConfigBuilder();
        long requestTimeout = resolveRequestTimeout();
        builder.setEndpoint(endpoint);
        builder.setRequestTimeout(Unsigned.uint(requestTimeout));
        builder.setCertificateValidator(resolveCertificateValidator());
        applyClientCertificates(builder, clientKeyMaterial);
        builder.setIdentityProvider(resolveIdentityProvider(endpoint, clientKeyMaterial));

        client = OpcUaClient.create(builder.build());
        long connectTimeout = resolveConnectTimeout();
        try {
            client.connectAsync().get(connectTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            cleanupClient();
            throw CollectorException.connectionException(
                    "OPC UA连接超时(" + connectTimeout + "ms)", currentDeviceId());
        } catch (Exception e) {
            cleanupClient();
            throw e;
        }

        connectionParams.put("endpointUrl", endpointUrl);
        connectionParams.put("securityPolicy", securityPolicy);
        connectionParams.put("securityMode", securityMode != null ? securityMode.name() : MessageSecurityMode.None.name());
        connectionParams.put("authType", resolveAuthType().name());
        connectionParams.put("requestTimeoutMs", requestTimeout);
        connectionParams.put("connectTimeoutMs", connectTimeout);

        cacheNamespaceInfo();
        metrics.setStatus(getStatus());
    }

    @Override
    protected void doDisconnect() {
        cleanupClient();
    }

    @Override
    protected void doHeartbeat() {
        if (client == null) {
            throw new IllegalStateException("OPC UA client is not connected");
        }
        try {
            NodeId timeNodeId = NodeId.parse("i=2258");
            DataValue value = client.readValue(0, TimestampsToReturn.Both, timeNodeId);
            if (value == null || value.getValue() == null) {
                throw new IllegalStateException("OPC UA heartbeat failed: No response from server");
            }
        } catch (Exception e) {
            throw new IllegalStateException("OPC UA heartbeat failed: " + e.getMessage(), e);
        }
    }

    @Override
    protected void doAuthenticate() {
        // OPC UA authentication handled during connection handshake.
    }

    @Override
    public OpcUaClient getClient() {
        return client;
    }

    private String resolveEndpointUrl() {
        if (config.getUrl() != null && !config.getUrl().isBlank()) {
            return config.getUrl();
        }
        String override = config.getStringConfig("endpointUrl",
                config.getStringConfig("endpoint", null));
        if (override != null && !override.isBlank()) {
            return override;
        }
        if (config.getHost() != null && !config.getHost().isBlank()) {
            int port = config.getPort() != null && config.getPort() > 0 ? config.getPort() : 4840;
            return "opc.tcp://" + config.getHost() + ":" + port;
        }
        throw new IllegalArgumentException("OPC UA endpointUrl is required");
    }

    private String resolveSecurityPolicy() {
        String text = config.getStringConfig("securityPolicy", "None");
        if (text.startsWith(OPC_POLICY_URI_PREFIX)) {
            return text;
        }
        for (SecurityPolicy value : SecurityPolicy.values()) {
            if (value.getUri().equalsIgnoreCase(text) || value.name().equalsIgnoreCase(text)) {
                return value.getUri();
            }
        }
        return text;
    }

    private MessageSecurityMode resolveSecurityMode() {
        String text = firstNonBlank(
                config.getStringConfig("securityMode", null),
                MessageSecurityMode.None.name());
        for (MessageSecurityMode value : MessageSecurityMode.values()) {
            if (value.name().equalsIgnoreCase(text)) {
                return value;
            }
        }
        return MessageSecurityMode.None;
    }

    private long resolveRequestTimeout() {
        Long timeout = config.getLongConfig("requestTimeoutMs", null);
        if (timeout == null || timeout <= 0) {
            timeout = config.getLongConfig("requestTimeout", null);
        }
        if ((timeout == null || timeout <= 0) && config.getReadTimeout() != null && config.getReadTimeout() > 0) {
            timeout = config.getReadTimeout().longValue();
        }
        if ((timeout == null || timeout <= 0) && config.getTimeout() != null && config.getTimeout() > 0) {
            timeout = config.getTimeout().longValue();
        }
        if (timeout == null || timeout <= 0) {
            timeout = MIN_TIMEOUT_MS;
        }
        return Math.max(MIN_TIMEOUT_MS, timeout);
    }

    private long resolveConnectTimeout() {
        Long timeout = config.getLongConfig("connectTimeoutMs", null);
        if ((timeout == null || timeout <= 0) && config.getConnectTimeout() != null && config.getConnectTimeout() > 0) {
            timeout = config.getConnectTimeout().longValue();
        }
        if (timeout == null || timeout <= 0) {
            timeout = resolveRequestTimeout();
        }
        return Math.max(MIN_TIMEOUT_MS, timeout);
    }

    private List<EndpointDescription> discoverEndpoints(String url) throws Exception {
        long timeout = Math.max(MIN_TIMEOUT_MS, resolveRequestTimeout());
        try {
            return DiscoveryClient.getEndpoints(url).get(timeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (!url.endsWith("/discovery")) {
                return DiscoveryClient.getEndpoints(url + "/discovery").get(timeout, TimeUnit.MILLISECONDS);
            }
            throw e;
        }
    }

    private EndpointDescription selectEndpoint(List<EndpointDescription> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            String deviceId = deviceInfo != null ? deviceInfo.getDeviceId() : null;
            throw new CollectorException("No OPC UA endpoints available", deviceId, null);
        }
        for (EndpointDescription endpoint : endpoints) {
            boolean policyMatch = endpoint.getSecurityPolicyUri() != null &&
                    endpoint.getSecurityPolicyUri().equalsIgnoreCase(securityPolicy);
            boolean modeMatch = securityMode == null || endpoint.getSecurityMode() == securityMode;
            if (policyMatch && modeMatch) {
                return endpoint;
            }
        }
        return endpoints.get(0);
    }

    private IdentityProvider resolveIdentityProvider(EndpointDescription endpoint, ClientKeyMaterial clientKeyMaterial) {
        AuthType authType = resolveAuthType();
        switch (authType) {
            case USERNAME -> {
                Credentials credentials = resolveCredentials();
                if (credentials.username() == null || credentials.username().isBlank()) {
                    throw CollectorException.configException("OPC UA用户名未配置", currentDeviceId(), null);
                }
                if (!supportsIdentityToken(endpoint, UserTokenType.UserName)) {
                    throw CollectorException.configException("OPC UA端点不支持USERNAME认证", currentDeviceId(), null);
                }
                return new UsernameProvider(credentials.username(), credentials.password() != null ? credentials.password() : "");
            }
            case CERT -> {
                if (clientKeyMaterial == null) {
                    throw CollectorException.configException("CERT认证需要配置clientCertPath", currentDeviceId(), null);
                }
                if (!supportsIdentityToken(endpoint, UserTokenType.Certificate)) {
                    throw CollectorException.configException("OPC UA端点不支持CERT认证", currentDeviceId(), null);
                }
                return new X509IdentityProvider(clientKeyMaterial.leafCertificate(), clientKeyMaterial.keyPair().getPrivate());
            }
            case ANONYMOUS -> {
                if (!supportsIdentityToken(endpoint, UserTokenType.Anonymous)) {
                    throw CollectorException.configException("OPC UA端点不支持匿名认证，请切换认证方式", currentDeviceId(), null);
                }
                return new AnonymousProvider();
            }
            default -> throw new IllegalStateException("Unsupported authType " + authType);
        }
    }

    private boolean supportsIdentityToken(EndpointDescription endpoint, UserTokenType targetType) {
        UserTokenPolicy[] policies = endpoint.getUserIdentityTokens();
        if (policies == null || policies.length == 0) {
            return targetType == UserTokenType.Anonymous;
        }
        for (UserTokenPolicy policy : policies) {
            if (policy != null && policy.getTokenType() == targetType) {
                return true;
            }
        }
        return false;
    }

    private void cleanupClient() {
        if (client != null) {
            try {
                client.disconnect();
            } catch (Exception e) {
                log.warn("Failed to disconnect OPC UA client", e);
            } finally {
                client = null;
            }
        }
    }

    private void cacheNamespaceInfo() {
        if (client == null) {
            return;
        }
        String namespaceUri = config.getStringConfig("namespaceUri", null);
        if (namespaceUri == null || namespaceUri.isBlank()) {
            return;
        }
        Number indexNumber = client.getNamespaceTable().getIndex(namespaceUri);
        if (indexNumber == null) {
            throw CollectorException.configException("服务器未暴露命名空间: " + namespaceUri, currentDeviceId(), null);
        }
        connectionParams.put("namespaceUri", namespaceUri);
        connectionParams.put("namespaceIndex", indexNumber.intValue());
    }

    private CertificateValidator resolveCertificateValidator() throws IOException {
        boolean trustAll = Boolean.TRUE.equals(config.getBoolConfig("trustAllServerCert", false));
        if (trustAll) {
            log.warn("OPC UA连接 {} 启用了信任所有服务端证书，仅建议在测试环境使用", currentDeviceId());
            return new CertificateValidator.InsecureCertificateValidator();
        }
        Path baseDir = resolveSecurityBaseDir();
        Path trustedDir = baseDir.resolve("trusted");
        Path rejectedDir = baseDir.resolve("rejected");
        Path issuersDir = baseDir.resolve("issuers");
        Path issuersRejectedDir = baseDir.resolve("issuers-rejected");
        Files.createDirectories(trustedDir);
        Files.createDirectories(rejectedDir);
        Files.createDirectories(issuersDir);
        Files.createDirectories(issuersRejectedDir);
        TrustListManager trustListManager =
                new FileBasedTrustListManager(trustedDir, rejectedDir, issuersDir, issuersRejectedDir);
        Path quarantineDir = baseDir.resolve("quarantine");
        Files.createDirectories(quarantineDir);
        CertificateQuarantine quarantine = new FileBasedCertificateQuarantine(quarantineDir.toFile());
        return new DefaultClientCertificateValidator(trustListManager, quarantine);
    }

    private Path resolveSecurityBaseDir() throws IOException {
        String key = config.getConnectionId();
        if (key == null || key.isBlank()) {
            key = currentDeviceId() != null ? currentDeviceId() : "default";
        }
        key = key.replaceAll("[^a-zA-Z0-9-_]", "_");
        Path baseDir = Paths.get("target", "opcua", "security", key);
        Files.createDirectories(baseDir);
        return baseDir;
    }

    private void applyClientCertificates(OpcUaClientConfigBuilder builder, ClientKeyMaterial material) {
        boolean secureChannel = securityPolicy != null &&
                !SecurityPolicy.None.getUri().equalsIgnoreCase(securityPolicy);
        if (material != null) {
            builder.setKeyPair(material.keyPair());
            builder.setCertificate(material.leafCertificate());
            builder.setCertificateChain(material.certificateChain());
        } else if (secureChannel) {
            throw CollectorException.configException(
                    "安全策略 " + securityPolicy + " 需要配置客户端证书", currentDeviceId(), null);
        }
    }

    private ClientKeyMaterial loadClientKeyMaterial() throws Exception {
        String pathText = config.getStringConfig("clientCertPath", null);
        if (pathText == null || pathText.isBlank()) {
            return null;
        }
        Path path = Paths.get(pathText).toAbsolutePath().normalize();
        if (!Files.exists(path)) {
            throw CollectorException.configException("客户端证书文件不存在: " + path, currentDeviceId(), null);
        }
        String fileName = path.getFileName().toString().toLowerCase();
        if (fileName.endsWith(".pfx") || fileName.endsWith(".p12")) {
            return loadPkcs12Material(path, config.getStringConfig("clientCertPassword", ""));
        }
        if (fileName.endsWith(".pem")) {
            return loadPemMaterial(path);
        }
        throw CollectorException.configException("不支持的客户端证书格式: " + path, currentDeviceId(), null);
    }

    private ClientKeyMaterial loadPkcs12Material(Path path, String password) throws Exception {
        char[] pwd = password != null ? password.toCharArray() : new char[0];
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(path)) {
            keyStore.load(input, pwd);
        }
        Enumeration<String> aliases = keyStore.aliases();
        if (!aliases.hasMoreElements()) {
            throw CollectorException.configException("PKCS12证书未包含条目: " + path, currentDeviceId(), null);
        }
        String alias = aliases.nextElement();
        Key key = keyStore.getKey(alias, pwd);
        if (!(key instanceof PrivateKey privateKey)) {
            throw CollectorException.configException("PKCS12证书缺少私钥: " + path, currentDeviceId(), null);
        }
        Certificate[] chain = keyStore.getCertificateChain(alias);
        if (chain == null || chain.length == 0) {
            Certificate certificate = keyStore.getCertificate(alias);
            if (certificate != null) {
                chain = new Certificate[]{certificate};
            }
        }
        if (chain == null || chain.length == 0) {
            throw CollectorException.configException("PKCS12证书缺少证书链: " + path, currentDeviceId(), null);
        }
        X509Certificate[] x509Chain = new X509Certificate[chain.length];
        for (int i = 0; i < chain.length; i++) {
            x509Chain[i] = (X509Certificate) chain[i];
        }
        return new ClientKeyMaterial(new KeyPair(x509Chain[0].getPublicKey(), privateKey), x509Chain);
    }

    private ClientKeyMaterial loadPemMaterial(Path path) throws Exception {
        String text = Files.readString(path, StandardCharsets.UTF_8);
        Matcher certMatcher = CERT_PATTERN.matcher(text);
        List<X509Certificate> certificates = new ArrayList<>();
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        while (certMatcher.find()) {
            byte[] certBytes = Base64.getMimeDecoder().decode(stripWhitespace(certMatcher.group(1)));
            certificates.add((X509Certificate) factory.generateCertificate(new ByteArrayInputStream(certBytes)));
        }
        Matcher keyMatcher = PRIVATE_KEY_PATTERN.matcher(text);
        if (certificates.isEmpty() || !keyMatcher.find()) {
            throw CollectorException.configException("PEM文件缺少证书或私钥: " + path, currentDeviceId(), null);
        }
        byte[] keyBytes = Base64.getMimeDecoder().decode(stripWhitespace(keyMatcher.group(1)));
        String algorithm = certificates.get(0).getPublicKey().getAlgorithm();
        KeyFactory keyFactory = KeyFactory.getInstance(algorithm);
        PrivateKey privateKey = keyFactory.generatePrivate(new PKCS8EncodedKeySpec(keyBytes));
        X509Certificate[] chain = certificates.toArray(new X509Certificate[0]);
        return new ClientKeyMaterial(new KeyPair(chain[0].getPublicKey(), privateKey), chain);
    }

    private String stripWhitespace(String text) {
        return text.replaceAll("\\s+", "");
    }

    private AuthType resolveAuthType() {
        String raw = config.getStringConfig("authType", AuthType.ANONYMOUS.name());
        if (raw == null || raw.isBlank()) {
            return AuthType.ANONYMOUS;
        }
        try {
            return AuthType.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException ex) {
            log.warn("Unknown authType '{}', fallback to ANONYMOUS", raw);
            return AuthType.ANONYMOUS;
        }
    }

    private Credentials resolveCredentials() {
        String username = config.getUsername();
        String password = config.getPassword();
        Map<String, String> authParams = config.getAuthParams();
        if ((username == null || username.isBlank()) && authParams != null) {
            username = authParams.get("username");
            password = authParams.get("password");
        }
        return new Credentials(username, password);
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private String currentDeviceId() {
        if (deviceInfo != null && deviceInfo.getDeviceId() != null) {
            return deviceInfo.getDeviceId();
        }
        return config.getDeviceId();
    }

    private record ClientKeyMaterial(KeyPair keyPair, X509Certificate[] certificateChain) {
        X509Certificate leafCertificate() {
            return certificateChain[0];
        }
    }

    private record Credentials(String username, String password) {}

    private enum AuthType {
        ANONYMOUS,
        USERNAME,
        CERT
    }
}
