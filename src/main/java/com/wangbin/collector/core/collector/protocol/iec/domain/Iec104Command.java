package com.wangbin.collector.core.collector.protocol.iec.domain;

import org.openmuc.j60870.ie.InformationElement;
import lombok.Builder;
import lombok.Data;

/**
 * IEC 104命令对象
 */
@Data
@Builder
public class Iec104Command {
    private int type;
    private int commonAddress;
    private InformationElement qualifier;
    private Object value;

    public static class Builder {
        private int type;
        private int commonAddress;
        private InformationElement qualifier;
        private Object value;

        public Builder type(int type) {
            this.type = type;
            return this;
        }

        public Builder commonAddress(int commonAddress) {
            this.commonAddress = commonAddress;
            return this;
        }

        public Builder qualifier(InformationElement qualifier) {
            this.qualifier = qualifier;
            return this;
        }

        public Builder value(Object value) {
            this.value = value;
            return this;
        }

        public Iec104Command build() {
            return new Iec104Command(type, commonAddress, qualifier, value);
        }
    }
}
