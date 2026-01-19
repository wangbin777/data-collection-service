package com.wangbin.collector.core.collector.protocol.iec.connection;

import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.collector.protocol.iec.domain.Iec104Config;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.openmuc.j60870.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

/**
 * IEC 104连接管理器
 */
@Getter
@Slf4j
public class Iec104ConnectionManager {

    /**
     * -- GETTER --
     *  获取连接
     */
    private Connection connection;
    /**
     * -- GETTER --
     *  获取配置
     */
    private Iec104Config config;

    public Iec104ConnectionManager() {
        this.config = new Iec104Config();
    }


    /**
     * 建立连接
     */
    public Connection connect(ConnectionEventListener listener) throws Exception {
        validateConfig();

        try {
            InetAddress address = InetAddress.getByName(config.getHost());
            ClientConnectionBuilder builder = new ClientConnectionBuilder(address);

            connection = builder
                    .setPort(config.getPort())
                    .setConnectionTimeout(config.getTimeout())
                    .setConnectionEventListener(listener)
                    .build();

            log.info("IEC 104连接建立成功: {}:{}", config.getHost(), config.getPort());
            return connection;
        } catch (Exception e) {
            log.error("IEC 104连接失败: {}:{}", config.getHost(), config.getPort(), e);
            throw new Exception("IEC 104连接失败: " + e.getMessage(), e);
        }
    }

    /**
     * 断开连接
     */
    public void disconnect() {
        if (connection != null) {
            try {
                connection.close();
                log.info("IEC 104连接已断开");
            } catch (Exception e) {
                log.error("断开IEC 104连接失败", e);
            } finally {
                connection = null;
            }
        }
    }

    /**
     * 发送ASDU
     */
    public void sendASdu(ASdu asdu) throws IOException {
        if (connection == null) {
            throw new IllegalStateException("连接未建立");
        }
        connection.send(asdu);
    }

    /**
     * 验证配置
     */
    private void validateConfig() {
        if (config.getHost() == null || config.getHost().isEmpty()) {
            throw new IllegalArgumentException("主机地址不能为空");
        }
    }

    /**
     * 获取整数值
     */
    private int getIntValue(Map<String, Object> map, String key, int defaultValue) {
        if (map.get(key) != null) {
            try {
                return Integer.parseInt(map.get(key).toString());
            } catch (NumberFormatException e) {
                log.warn("配置项{}格式错误，使用默认值{}", key, defaultValue);
            }
        }
        return defaultValue;
    }
}
