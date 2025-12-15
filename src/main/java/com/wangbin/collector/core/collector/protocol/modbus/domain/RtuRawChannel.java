package com.wangbin.collector.core.collector.protocol.modbus.domain;

import com.fazecast.jSerialComm.SerialPort;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class RtuRawChannel {

    private final SerialPort port;

    public RtuRawChannel(String portName, int baud, int dataBits,
                         int stopBits, int parity) {

        port = SerialPort.getCommPort(portName);
        port.setComPortParameters(baud, dataBits, stopBits, parity);
        port.setComPortTimeouts(
                SerialPort.TIMEOUT_READ_BLOCKING,
                1000,
                0
        );

        if (!port.openPort()) {
            throw new IllegalStateException("串口打开失败: " + portName);
        }
    }

    public synchronized byte[] send(byte[] request, int respLen)
            throws Exception {

        port.writeBytes(request, request.length);

        byte[] resp = new byte[respLen];
        int read = port.readBytes(resp, respLen);

        if (read <= 0) {
            throw new TimeoutException("RTU 无响应");
        }

        return Arrays.copyOf(resp, read);
    }

    public void close() {
        port.closePort();
    }
}

