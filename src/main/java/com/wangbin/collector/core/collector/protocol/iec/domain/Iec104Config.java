package com.wangbin.collector.core.collector.protocol.iec.domain;

import lombok.Data;

/**
 * IEC 104配置对象
 */
@Data
public class Iec104Config {
    private String host;
    private int port = 2404;
    private int commonAddress = 1;
    private int timeout = 5000;
}
