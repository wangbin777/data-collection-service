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
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.enumerated.UserTokenType;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.UserTokenPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * OPC UA connection adapter backed by Milo {@link OpcUaClient}.
 */
@Slf4j
public class OpcUaConnectionAdapter extends AbstractConnectionAdapter<OpcUaClient> {

    private static final String OPC_POLICY_URI_PREFIX = "http://opcfoundation.org/UA/SecurityPolicy#";

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

        OpcUaClientConfigBuilder builder = new OpcUaClientConfigBuilder();
        builder.setEndpoint(endpoint);
        builder.setRequestTimeout(Unsigned.uint(resolveRequestTimeout()));
        builder.setIdentityProvider(resolveIdentityProvider(endpoint));

        client = OpcUaClient.create(builder.build());
        long connectTimeout = resolveConnectTimeout();
        client.connect();

        connectionParams.put("endpointUrl", endpointUrl);
        connectionParams.put("securityPolicy", securityPolicy);
        connectionParams.put("securityMode", securityMode != null ? securityMode.name() : MessageSecurityMode.None.name());
        metrics.setStatus(getStatus());
    }

    @Override
    protected void doDisconnect() throws Exception {
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

    @Override
    protected void doHeartbeat() {
        if (client == null) {
            throw new IllegalStateException("OPC UA client is not connected");
        }
        // Send a simple request to verify connection is working
        try {
            // Use server time node reading as heartbeat check
            // OPC UA standard node: ServerStatus.CurrentTime
            NodeId timeNodeId = NodeId.parse("i=2258");
            DataValue value =
                client.readValue(0, TimestampsToReturn.Both, timeNodeId);
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
            /*int port = config.getPort() != null && config.getPort() > 0 ? config.getPort() : 4840;
            return "opc.tcp://" + config.getHost() + ":" + port;*/
            return config.getHost();
        }
        throw new IllegalArgumentException("OPC UA endpointUrl is required");
    }

    private String resolveSecurityPolicy() {
        String text = config.getStringConfig("securityPolicy", "None");
        if (text.startsWith(OPC_POLICY_URI_PREFIX)) {
            return text;
        }
        for (SecurityPolicy value : SecurityPolicy.values()) {
            if (value.getUri().equalsIgnoreCase(text) ||
                    value.name().equalsIgnoreCase(text)) {
                return value.getUri();
            }
        }
        return text;
    }

    private MessageSecurityMode resolveSecurityMode() {
        String text = config.getStringConfig("messageMode", "None");
        if (text == null || text.isBlank()) {
            return MessageSecurityMode.None;
        }
        for (MessageSecurityMode value : MessageSecurityMode.values()) {
            if (value.name().equalsIgnoreCase(text)) {
                return value;
            }
        }
        return MessageSecurityMode.None;
    }

    private long resolveRequestTimeout() {
        if (config.getReadTimeout() != null && config.getReadTimeout() > 0) {
            return config.getReadTimeout();
        }
        if (config.getTimeout() != null && config.getTimeout() > 0) {
            return config.getTimeout();
        }
        Long timeout = config.getLongConfig("requestTimeout", null);
        return timeout != null && timeout > 0 ? timeout : 5000;
    }

    private long resolveConnectTimeout() {
        if (config.getConnectTimeout() != null && config.getConnectTimeout() > 0) {
            return config.getConnectTimeout();
        }
        return Math.max(5000, (int) resolveRequestTimeout());
    }

    private List<EndpointDescription> discoverEndpoints(String url) throws Exception {
        long timeout = Math.max(5000, resolveRequestTimeout());
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
            boolean modeMatch = securityMode == null ||
                    endpoint.getSecurityMode() == securityMode;
            if (policyMatch && modeMatch) {
                return endpoint;
            }
        }
        return endpoints.get(0);
    }

    private IdentityProvider resolveIdentityProvider(EndpointDescription endpoint) {
        String username = config.getUsername();
        String password = config.getPassword();
        if ((username == null || username.isBlank()) && config.getAuthParams() != null) {
            username = config.getAuthParams().get("username");
            password = config.getAuthParams().get("password");
        }
        if (username != null && !username.isBlank() && supportsUsernameToken(endpoint)) {
            return new UsernameProvider(username, password != null ? password : "");
        }
        return new AnonymousProvider();
    }

    private boolean supportsUsernameToken(EndpointDescription endpoint) {
        UserTokenPolicy[] policies = endpoint.getUserIdentityTokens();
        if (policies == null) {
            return false;
        }
        for (UserTokenPolicy policy : policies) {
            if (policy != null && policy.getTokenType() == UserTokenType.UserName) {
                return true;
            }
        }
        return false;
    }

}








