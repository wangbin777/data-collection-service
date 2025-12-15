package com.wangbin.collector.common.domain.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * 数据上报消息
 */
@Data
public class ReportMessage {

    private String reportId;
    private Long timestamp;
    private String reportType; // REALTIME, HISTORY, ALARM, etc.
    private String source; // COLLECTOR, PROCESSOR, CACHE, etc.
    private List<DataItem> dataItems;
    private Map<String, Object> metadata;

    @Data
    public static class DataItem {
        private String pointId;
        private String deviceId;
        private String pointName;
        private String pointAlias;
        private Object value;
        private Object rawValue;
        private String dataType;
        private Integer quality;
        private String unit;
        private Long collectTime;
        private Long processTime;
        private Map<String, Object> tags;

        public DataItem() {
            this.collectTime = System.currentTimeMillis();
            this.processTime = System.currentTimeMillis();
            this.quality = 100;
        }
    }

    public void addDataItem(DataItem item) {
        if (dataItems == null) {
            dataItems = new java.util.ArrayList<>();
        }
        dataItems.add(item);
    }

    public void addMetadata(String key, Object value) {
        if (metadata == null) {
            metadata = new java.util.HashMap<>();
        }
        metadata.put(key, value);
    }
}
