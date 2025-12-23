package com.wangbin.collector.core.collector.protocol.opc.ua.domain;

import lombok.Getter;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;

/**
 * Holder for parsed OPC UA addressing metadata.
 */
@Getter
public class OpcUaAddress {

    private final NodeId nodeId;
    private final OpcUaDataType dataType;
    private final double samplingInterval;
    private final int queueSize;
    private final double deadband;
    private final boolean subscribe;

    public OpcUaAddress(NodeId nodeId,
                        OpcUaDataType dataType,
                        double samplingInterval,
                        int queueSize,
                        double deadband,
                        boolean subscribe) {
        this.nodeId = nodeId;
        this.dataType = dataType;
        this.samplingInterval = samplingInterval;
        this.queueSize = queueSize;
        this.deadband = deadband;
        this.subscribe = subscribe;
    }

    public NodeId toNodeId() {
        return nodeId;
    }

    public boolean needSubscribe() {
        return subscribe;
    }
}

