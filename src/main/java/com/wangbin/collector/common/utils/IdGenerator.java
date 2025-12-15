package com.wangbin.collector.common.utils;

import lombok.extern.slf4j.Slf4j;

import java.net.NetworkInterface;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Date;
import java.util.Enumeration;

/**
 * ID生成器工具类
 */
@Slf4j
public class IdGenerator {

    // 雪花算法参数
    private static final long EPOCH = 1609459200000L; // 2021-01-01 00:00:00
    private static final long WORKER_ID_BITS = 5L;
    private static final long DATACENTER_ID_BITS = 5L;
    private static final long SEQUENCE_BITS = 12L;

    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);
    private static final long MAX_DATACENTER_ID = ~(-1L << DATACENTER_ID_BITS);
    private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS);

    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;

    private static final SnowflakeIdWorker snowflakeIdWorker = new SnowflakeIdWorker();

    private IdGenerator() {
        // 工具类，防止实例化
    }

    /**
     * 生成雪花算法ID
     */
    public static long generateSnowflakeId() {
        return snowflakeIdWorker.nextId();
    }

    /**
     * 生成雪花算法ID字符串
     */
    public static String generateSnowflakeIdStr() {
        return String.valueOf(snowflakeIdWorker.nextId());
    }

    /**
     * 生成UUID（不带横线）
     */
    public static String generateUuid() {
        return java.util.UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 生成UUID（带横线）
     */
    public static String generateUuidWithDash() {
        return java.util.UUID.randomUUID().toString();
    }

    /**
     * 生成设备ID
     */
    public static String generateDeviceId(String prefix) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String random = String.valueOf((int)(Math.random() * 10000));
        return prefix + "_" + timestamp + "_" + random;
    }

    /**
     * 生成数据点ID
     */
    public static String generateDataPointId(String deviceId, String pointName) {
        String hash = CryptoUtil.md5(deviceId + pointName).substring(0, 8);
        return deviceId + "_" + pointName + "_" + hash;
    }

    /**
     * 生成任务ID
     */
    public static String generateTaskId(String type) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String random = String.valueOf((int)(Math.random() * 1000000));
        return type + "_" + timestamp + "_" + random;
    }

    /**
     * 生成消息ID
     */
    public static String generateMessageId() {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String random = String.valueOf((int)(Math.random() * 10000));
        return "MSG_" + timestamp + "_" + random;
    }

    /**
     * 生成批次ID
     */
    public static String generateBatchId() {
        String date = DateUtil.format(new Date(), "yyyyMMddHHmmss");
        String random = String.valueOf((int)(Math.random() * 1000));
        return "BATCH_" + date + "_" + random;
    }

    /**
     * 生成连接ID
     */
    public static String generateConnectionId(String deviceId) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String hash = CryptoUtil.md5(deviceId + timestamp).substring(0, 6);
        return "CONN_" + deviceId + "_" + hash;
    }

    /**
     * 生成报告ID
     */
    public static String generateReportId() {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String random = String.valueOf((int)(Math.random() * 10000));
        return "REPORT_" + timestamp + "_" + random;
    }

    /**
     * 生成告警ID
     */
    public static String generateAlertId() {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String random = String.valueOf((int)(Math.random() * 10000));
        return "ALERT_" + timestamp + "_" + random;
    }

    /**
     * 生成短ID（8位）
     */
    public static String generateShortId() {
        String uuid = generateUuid();
        return uuid.substring(0, 8);
    }

    /**
     * 生成随机数字ID
     */
    public static String generateRandomNumberId(int length) {
        StringBuilder sb = new StringBuilder();
        SecureRandom random = new SecureRandom();

        for (int i = 0; i < length; i++) {
            sb.append(random.nextInt(10));
        }

        return sb.toString();
    }

    /**
     * 生成递增ID（基于时间戳和序列号）
     */
    public static String generateIncrementalId(String prefix) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String sequence = getSequence();
        return prefix + timestamp + sequence;
    }

    private static String sequence = "0000";

    private static synchronized String getSequence() {
        int seq = Integer.parseInt(sequence);
        seq++;
        if (seq > 9999) {
            seq = 0;
        }
        sequence = String.format("%04d", seq);
        return sequence;
    }

    /**
     * 雪花算法ID生成器
     */
    private static class SnowflakeIdWorker {
        private final long workerId;
        private final long datacenterId;
        private long sequence = 0L;
        private long lastTimestamp = -1L;

        public SnowflakeIdWorker() {
            this.workerId = getWorkerId();
            this.datacenterId = getDatacenterId();

            if (workerId > MAX_WORKER_ID || workerId < 0) {
                throw new IllegalArgumentException(
                        String.format("worker Id can't be greater than %d or less than 0", MAX_WORKER_ID));
            }
            if (datacenterId > MAX_DATACENTER_ID || datacenterId < 0) {
                throw new IllegalArgumentException(
                        String.format("datacenter Id can't be greater than %d or less than 0", MAX_DATACENTER_ID));
            }
        }

        public synchronized long nextId() {
            long timestamp = timeGen();

            if (timestamp < lastTimestamp) {
                throw new RuntimeException(
                        String.format("Clock moved backwards. Refusing to generate id for %d milliseconds",
                                lastTimestamp - timestamp));
            }

            if (lastTimestamp == timestamp) {
                sequence = (sequence + 1) & MAX_SEQUENCE;
                if (sequence == 0) {
                    timestamp = tilNextMillis(lastTimestamp);
                }
            } else {
                sequence = 0L;
            }

            lastTimestamp = timestamp;

            return ((timestamp - EPOCH) << TIMESTAMP_SHIFT) |
                    (datacenterId << DATACENTER_ID_SHIFT) |
                    (workerId << WORKER_ID_SHIFT) |
                    sequence;
        }

        private long tilNextMillis(long lastTimestamp) {
            long timestamp = timeGen();
            while (timestamp <= lastTimestamp) {
                timestamp = timeGen();
            }
            return timestamp;
        }

        private long timeGen() {
            return System.currentTimeMillis();
        }

        private long getWorkerId() {
            try {
                String hostname = java.net.InetAddress.getLocalHost().getHostName();
                int code = hostname.hashCode();
                return code & MAX_WORKER_ID;
            } catch (Exception e) {
                return 1L;
            }
        }

        private long getDatacenterId() {
            try {
                Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
                while (networkInterfaces.hasMoreElements()) {
                    NetworkInterface networkInterface = networkInterfaces.nextElement();
                    byte[] mac = networkInterface.getHardwareAddress();
                    if (mac != null) {
                        int id = ((0x000000FF & (int) mac[mac.length - 1]) |
                                (0x0000FF00 & (((int) mac[mac.length - 2]) << 8))) >> 6;
                        return id & MAX_DATACENTER_ID;
                    }
                }
            } catch (Exception e) {
                // ignore
            }
            return 1L;
        }
    }
}
