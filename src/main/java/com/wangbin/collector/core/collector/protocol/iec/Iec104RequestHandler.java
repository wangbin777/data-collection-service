package com.wangbin.collector.core.collector.protocol.iec;

import lombok.extern.slf4j.Slf4j;
import org.openmuc.j60870.ASdu;
import org.openmuc.j60870.ASduType;
import org.openmuc.j60870.ie.*;

import java.util.Map;
import java.util.concurrent.*;

/**
 * IEC 104请求响应处理器
 */
@Slf4j
public class Iec104RequestHandler {

    private final Map<Integer, CompletableFuture<Object>> pendingRequests = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1);
    private final long defaultTimeout = 5000;  // 默认超时5秒

    /**
     * 发送读取请求并等待响应
     */
    public CompletableFuture<Object> sendReadRequest(int ioAddress, String requestType) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        int requestId = generateRequestId(ioAddress, requestType);

        pendingRequests.put(requestId, future);

        // 设置超时
        timeoutScheduler.schedule(() -> {
            if (pendingRequests.remove(requestId) != null) {
                future.completeExceptionally(new TimeoutException("请求超时"));
            }
        }, defaultTimeout, TimeUnit.MILLISECONDS);

        return future;
    }

    /**
     * 处理收到的ASDU响应
     */
    public void handleResponse(ASdu asdu) {
        try {
            // 解析响应中的信息对象
            InformationObject[] ios = asdu.getInformationObjects();
            if (ios == null || ios.length == 0) {
                return;
            }

            for (InformationObject io : ios) {
                int ioAddress = io.getInformationObjectAddress();
                InformationElement[][] elements = io.getInformationElements();

                if (elements != null && elements.length > 0) {
                    Object value = parseInformationElements(elements[0], asdu.getTypeIdentification());
                    completeRequest(ioAddress, asdu.getTypeIdentification(), value);
                }
            }
        } catch (Exception e) {
            log.error("处理IEC 104响应失败", e);
        }
    }

    /**
     * 解析信息元素
     */
    private Object parseInformationElements(InformationElement[] elements, ASduType aSduType) {
        if (elements == null || elements.length == 0) {
            return null;
        }

        switch (aSduType) {
            case M_SP_NA_1:
                if (elements[0] instanceof IeSinglePointWithQuality) {
                    IeSinglePointWithQuality sp = (IeSinglePointWithQuality) elements[0];
                    return sp.isOn();
                }
                break;

            case M_DP_NA_1:
                if (elements[0] instanceof IeDoublePointWithQuality) {
                    IeDoublePointWithQuality dp = (IeDoublePointWithQuality) elements[0];
                    return dp.getDoublePointInformation().name();
                }
                break;

            case M_ME_NA_1:
                if (elements[0] instanceof IeNormalizedValue) {
                    IeNormalizedValue nv = (IeNormalizedValue) elements[0];
                    return nv.getUnnormalizedValue();
                }
                break;

            case M_ME_NB_1:
                if (elements[0] instanceof IeScaledValue) {
                    IeScaledValue sv = (IeScaledValue) elements[0];
                    return sv.getUnnormalizedValue();
                }
                break;

            case M_ME_NC_1:
                if (elements[0] instanceof IeShortFloat) {
                    IeShortFloat sf = (IeShortFloat) elements[0];
                    return sf.getValue();
                }
                break;
        }

        return null;
    }

    /**
     * 完成请求
     */
    private void completeRequest(int ioAddress, ASduType aSduType, Object value) {
        int requestId = generateRequestId(ioAddress, String.valueOf(aSduType.getId()));
        CompletableFuture<Object> future = pendingRequests.remove(requestId);

        if (future != null) {
            future.complete(value);
        }
    }

    /**
     * 生成请求ID
     */
    private int generateRequestId(int ioAddress, String requestType) {
        return (ioAddress << 16) | requestType.hashCode();
    }

    /**
     * 清理资源
     */
    public void cleanup() {
        pendingRequests.forEach((key, future) -> {
            future.completeExceptionally(new Exception("处理器已关闭"));
        });
        pendingRequests.clear();
        timeoutScheduler.shutdown();
    }
}