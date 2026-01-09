package com.wangbin.collector.core.collector.scheduler;

/**
 * 设备调度信息
 */
class DeviceScheduleInfo {

    private final String deviceId;
    private final long startTime;
    private volatile boolean running;

    DeviceScheduleInfo(String deviceId, boolean running) {
        this.deviceId = deviceId;
        this.running = running;
        this.startTime = System.currentTimeMillis();
    }

    String getDeviceId() {
        return deviceId;
    }

    long getStartTime() {
        return startTime;
    }

    boolean isRunning() {
        return running;
    }

    void setRunning(boolean running) {
        this.running = running;
    }
}
