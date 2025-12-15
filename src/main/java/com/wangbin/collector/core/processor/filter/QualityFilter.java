package com.wangbin.collector.core.processor.filter;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.processor.AbstractDataProcessor;
import com.wangbin.collector.core.processor.ProcessContext;
import com.wangbin.collector.core.processor.ProcessResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 质量过滤器
 * 根据数据质量过滤数据
 */
@Slf4j
@Component
public class QualityFilter extends AbstractDataProcessor {

    /**
     * 最小质量阈值（0-100）
     */
    private int minQuality = 60;

    public QualityFilter() {
        this.name = "QualityFilter";
        this.type = "FILTER";
        this.description = "质量过滤器";
        this.priority = 50;
    }

    @Override
    protected void doInit() throws Exception {
        log.info("质量过滤器初始化完成: {}", getName());
    }

    @Override
    protected ProcessResult doProcess(ProcessContext context, DataPoint point, Object rawValue) throws Exception {
        // 获取上下文中的原始数据质量
        int rawQuality = context.getRawQuality();

        // 判断数据质量
        if (rawQuality < minQuality) {
            return ProcessResult.skip(rawValue,
                    String.format("数据质量 %d 低于阈值 %d", rawQuality, minQuality));
        }

        // 检查是否有处理历史记录
        if (context.getProcessHistory() != null && !context.getProcessHistory().isEmpty()) {
            // 检查所有处理器的质量
            for (ProcessResult history : context.getProcessHistory().values()) {
                if (history.getQuality() < minQuality) {
                    return ProcessResult.skip(rawValue,
                            String.format("处理历史质量 %d 低于阈值 %d", history.getQuality(), minQuality));
                }
            }
        }

        return ProcessResult.success(rawValue, rawValue,
                String.format("数据质量 %d 符合要求", rawQuality));
    }

    @Override
    protected void loadConfig(Map<String, Object> config) {
        super.loadConfig(config);
        minQuality = getIntConfig("minQuality", 60);
    }

    @Override
    protected void doDestroy() throws Exception {
        log.info("质量过滤器销毁完成: {}", getName());
    }
}