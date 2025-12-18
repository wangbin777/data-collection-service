package com.wangbin.collector.core.collector.protocol.iec.factory;

import org.openmuc.j60870.*;
import org.openmuc.j60870.ie.*;

/**
 * IEC 104 ASDU工厂
 */
public class Iec104AsduFactory {

    /**
     * 创建单点命令ASDU
     */
    public static ASdu createSingleCommandAsdu(int commonAddress, int ioAddress, boolean value) {
        IeSingleCommand command = new IeSingleCommand(value, 0, false);
        return createCommandAsdu(ASduType.C_SC_NA_1, commonAddress, ioAddress, command);
    }

    /**
     * 创建双点命令ASDU
     */
    public static ASdu createDoubleCommandAsdu(int commonAddress, int ioAddress,
                                               IeDoubleCommand.DoubleCommandState state) {
        IeDoubleCommand command = new IeDoubleCommand(state, 0, false);
        return createCommandAsdu(ASduType.C_DC_NA_1, commonAddress, ioAddress, command);
    }

    /**
     * 创建设点命令ASDU
     */
    public static ASdu createSetpointCommandAsdu(int commonAddress, int ioAddress,
                                                 int typeId, float value) {
        SetpointCommandInfo commandInfo = createSetpointCommandInfo(typeId, value);
        InformationElement[][] elements = new InformationElement[1][];
        elements[0] = new InformationElement[] { commandInfo.element, commandInfo.qualifier };

        InformationObject[] ios = new InformationObject[] {
                new InformationObject(ioAddress, elements)
        };

        return new ASdu(commandInfo.asduType, false,
                CauseOfTransmission.ACTIVATION, false, false,
                commonAddress, 1, ios);
    }

    /**
     * 创建通用命令ASDU
     */
    public static ASdu createCommandAsdu(ASduType asduType, int commonAddress,
                                         int ioAddress, InformationElement commandElement) {
        InformationElement[][] elements = new InformationElement[1][];
        elements[0] = new InformationElement[] { commandElement };

        InformationObject[] ios = new InformationObject[] {
                new InformationObject(ioAddress, elements)
        };

        return new ASdu(asduType, false,
                CauseOfTransmission.ACTIVATION, false, false,
                commonAddress, 1, ios);
    }

    /**
     * 创建设点命令信息
     */
    private static SetpointCommandInfo createSetpointCommandInfo(int typeId, float value) {
        int ql = 0;
        boolean select = false;

        switch (typeId) {
            case 48: // TYPE_SETPOINT_NORMALIZED
                return new SetpointCommandInfo(
                        new IeNormalizedValue(value),
                        ASduType.C_SE_NA_1,
                        new IeQualifierOfSetPointCommand(ql, select)
                );
            case 49: // TYPE_SETPOINT_SCALED
                return new SetpointCommandInfo(
                        new IeScaledValue((int) value),
                        ASduType.C_SE_NB_1,
                        new IeQualifierOfSetPointCommand(ql, select)
                );
            case 50: // TYPE_SETPOINT_FLOAT
                return new SetpointCommandInfo(
                        new IeShortFloat(value),
                        ASduType.C_SE_NC_1,
                        new IeQualifierOfSetPointCommand(ql, select)
                );
            default:
                throw new IllegalArgumentException("不支持的设点命令类型: " + typeId);
        }
    }

    /**
     * 设点命令信息内部类
     */
    private static class SetpointCommandInfo {
        final InformationElement element;
        final ASduType asduType;
        final IeQualifierOfSetPointCommand qualifier;

        SetpointCommandInfo(InformationElement element, ASduType asduType,
                            IeQualifierOfSetPointCommand qualifier) {
            this.element = element;
            this.asduType = asduType;
            this.qualifier = qualifier;
        }
    }


    /**
     * 创建单点信息读取命令
     */
    public static ASdu createSinglePointReadCommand(int commonAddress, int ioAddress) {
        InformationElement[][] elements = new InformationElement[1][];
        elements[0] = new InformationElement[0];  // 读取命令不需要信息元素

        InformationObject[] ios = new InformationObject[] {
                new InformationObject(ioAddress, elements)
        };

        return new ASdu(
                ASduType.C_RD_NA_1,
                false,  // 测试标志
                CauseOfTransmission.REQUEST,  // 传输原因为请求
                false,  // 否定确认
                false,  // 响应标志
                commonAddress,
                1,
                ios
        );
    }

    /**
     * 创建双点信息读取命令
     */
    public static ASdu createDoublePointReadCommand(int commonAddress, int ioAddress) {
        return createSinglePointReadCommand(commonAddress, ioAddress);  // 命令格式相同
    }

    /**
     * 创建测量值读取命令
     */
    public static ASdu createMeasuredValueReadCommand(int commonAddress, int ioAddress) {
        return createSinglePointReadCommand(commonAddress, ioAddress);  // 命令格式相同
    }

    /**
     * 创建总召唤命令
     */
    public static ASdu createGeneralInterrogationCommand(int commonAddress) {
        InformationElement[][] elements = new InformationElement[1][];
        elements[0] = new InformationElement[] {
                new IeQualifierOfInterrogation(20)  // 20表示总召唤
        };

        InformationObject[] ios = new InformationObject[] {
                new InformationObject(0, elements)  // 地址为0
        };

        return new ASdu(
                ASduType.C_IC_NA_1,
                false,
                CauseOfTransmission.ACTIVATION,
                false,
                false,
                commonAddress,
                1,
                ios
        );
    }

    /**
     * 创建读单个信息对象命令（通用方法）
     */
    public static ASdu createReadCommand(int commonAddress, int ioAddress, ASduType asduType) {
        InformationElement[][] elements = new InformationElement[1][];
        elements[0] = new InformationElement[0];

        InformationObject[] ios = new InformationObject[] {
                new InformationObject(ioAddress, elements)
        };

        return new ASdu(
                asduType,
                false,
                CauseOfTransmission.REQUEST,
                false,
                false,
                commonAddress,
                1,
                ios
        );
    }
}
