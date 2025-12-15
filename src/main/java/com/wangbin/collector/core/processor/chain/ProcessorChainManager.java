package com.wangbin.collector.core.processor.chain;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.processor.DataProcessor;
import com.wangbin.collector.core.processor.ProcessContext;
import com.wangbin.collector.core.processor.ProcessResult;
import com.wangbin.collector.core.processor.manager.DataProcessorManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 处理器链管理器
 * 负责管理多个处理器链，每个链包含有序的处理器节点
 */
@Slf4j
@Component
public class ProcessorChainManager {

    @Autowired
    private DataProcessorManager dataProcessorManager;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    /**
     * 处理器链集合
     * key: 链名称, value: 处理器链
     */
    private final Map<String, DataProcessorChain> chains = new ConcurrentHashMap<>();

    /**
     * 默认链名称
     */
    private static final String DEFAULT_CHAIN_NAME = "default";

    /**
     * 初始化
     */
    @PostConstruct
    public void init() {
        log.info("初始化处理器链管理器...");

        // 创建默认处理器链
        createDefaultChain();

        // 加载配置中的处理器链
        loadChainsFromConfig();

        // 启动监控
        startChainMonitor();

        log.info("处理器链管理器初始化完成，共 {} 个处理器链", chains.size());
    }

    /**
     * 销毁
     */
    @PreDestroy
    public void destroy() {
        log.info("销毁处理器链管理器...");

        // 停止所有处理器链
        chains.values().forEach(chain -> {
            if (chain.isRunning()) {
                chain.stop();
            }
        });

        // 清空处理器链
        chains.clear();

        log.info("处理器链管理器销毁完成");
    }

    /**
     * 创建默认处理器链
     */
    private void createDefaultChain() {
        DataProcessorChain chain = new DataProcessorChain();
        chain.setName(DEFAULT_CHAIN_NAME);
        chain.setDescription("默认数据处理链");
        chain.setEnabled(true);
        chain.setContinueOnError(false); // 默认出错停止

        // 默认处理器节点配置
        List<ProcessorNode> nodes = new ArrayList<>();

        // 1. 数据验证器
        nodes.add(new ProcessorNode("DataValidator", "数据验证器", 10, true));

        // 2. 数据转换器
        nodes.add(new ProcessorNode("DataConverter", "数据转换器", 20, true));

        // 3. 单位转换器
        nodes.add(new ProcessorNode("UnitConverter", "单位转换器", 30, true));

        // 4. 死区过滤器
        nodes.add(new ProcessorNode("DeadbandFilter", "死区过滤器", 40, true));

        // 5. 质量过滤器
        nodes.add(new ProcessorNode("QualityFilter", "质量过滤器", 50, true));

        // 6. 数据计算器
        nodes.add(new ProcessorNode("DataCalculator", "数据计算器", 60, true));

        chain.setNodes(nodes);
        chain.setDataProcessorManager(dataProcessorManager);

        // 注册默认链
        chains.put(DEFAULT_CHAIN_NAME, chain);

        log.info("创建默认处理器链: {}", DEFAULT_CHAIN_NAME);
    }

    /**
     * 从配置加载处理器链
     */
    private void loadChainsFromConfig() {
        // TODO: 从数据库或配置文件加载处理器链配置
        // 这里可以加载用户自定义的处理器链

        // 示例：创建一个快速处理链
        DataProcessorChain fastChain = new DataProcessorChain();
        fastChain.setName("fast");
        fastChain.setDescription("快速处理链（只包含必要处理器）");
        fastChain.setEnabled(true);
        fastChain.setContinueOnError(true); // 快速链出错继续执行

        List<ProcessorNode> fastNodes = new ArrayList<>();
        fastNodes.add(new ProcessorNode("DataValidator", "数据验证器", 10, true));
        fastNodes.add(new ProcessorNode("DataConverter", "数据转换器", 20, true));
        fastNodes.add(new ProcessorNode("DeadbandFilter", "死区过滤器", 30, true));

        fastChain.setNodes(fastNodes);
        fastChain.setDataProcessorManager(dataProcessorManager);

        chains.put("fast", fastChain);
        log.info("加载快速处理器链: fast");

        // 示例：创建一个质量检查链
        DataProcessorChain qualityChain = new DataProcessorChain();
        qualityChain.setName("quality");
        qualityChain.setDescription("质量检查链");
        qualityChain.setEnabled(true);
        qualityChain.setContinueOnError(false);

        List<ProcessorNode> qualityNodes = new ArrayList<>();
        qualityNodes.add(new ProcessorNode("DataValidator", "数据验证器", 10, true));
        qualityNodes.add(new ProcessorNode("QualityFilter", "质量过滤器", 20, true));

        qualityChain.setNodes(qualityNodes);
        qualityChain.setDataProcessorManager(dataProcessorManager);

        chains.put("quality", qualityChain);
        log.info("加载质量检查处理器链: quality");
    }

    /**
     * 启动链监控
     */
    private void startChainMonitor() {
        // 可以定期监控处理器链的状态和性能
        // 这里可以集成Metrics或自定义监控
    }

    /**
     * 执行处理器链
     *
     * @param chainName 处理器链名称
     * @param context   处理上下文
     * @param point     数据点
     * @param rawValue  原始值
     * @return 处理结果
     */
    public ProcessResult executeChain(String chainName, ProcessContext context,
                                      DataPoint point, Object rawValue) {
        if (chainName == null) {
            chainName = DEFAULT_CHAIN_NAME;
        }

        DataProcessorChain chain = chains.get(chainName);
        if (chain == null) {
            log.warn("处理器链不存在: {}，使用默认链", chainName);
            chain = chains.get(DEFAULT_CHAIN_NAME);

            if (chain == null) {
                log.error("默认处理器链不存在，无法执行处理");
                return ProcessResult.error(rawValue, "处理器链配置错误");
            }
        }

        if (!chain.isEnabled()) {
            log.warn("处理器链已禁用: {}", chainName);
            return ProcessResult.skip(rawValue, "处理器链已禁用");
        }

        log.debug("开始执行处理器链: chain={}, point={}", chainName, point.getPointName());

        try {
            ProcessResult result = chain.execute(context, point, rawValue);

            // 发布处理完成事件
            eventPublisher.publishEvent(new ChainExecutionEvent(
                    this, chainName, point.getPointId(), result.isSuccess(),
                    result.getProcessingTime()
            ));

            return result;
        } catch (Exception e) {
            log.error("处理器链执行异常: chain={}, point={}", chainName, point.getPointName(), e);
            return ProcessResult.error(rawValue, "处理器链执行异常: " + e.getMessage());
        }
    }

    /**
     * 批量执行处理器链
     *
     * @param chainName   处理器链名称
     * @param context     处理上下文
     * @param dataPoints  数据点映射
     * @return 处理结果映射
     */
    public Map<String, ProcessResult> batchExecuteChain(String chainName, ProcessContext context,
                                                        Map<DataPoint, Object> dataPoints) {
        if (dataPoints == null || dataPoints.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, ProcessResult> results = new HashMap<>(dataPoints.size());

        if (chains.containsKey(chainName)) {
            DataProcessorChain chain = chains.get(chainName);
            if (chain.isEnabled() && chain.isRunning()) {
                // 使用链的批量执行方法（如果有）
                return chain.batchExecute(context, dataPoints);
            }
        }

        // 逐个执行
        for (Map.Entry<DataPoint, Object> entry : dataPoints.entrySet()) {
            ProcessResult result = executeChain(chainName, context, entry.getKey(), entry.getValue());
            results.put(entry.getKey().getPointId(), result);
        }

        return results;
    }

    /**
     * 注册处理器链
     *
     * @param chain 处理器链
     */
    public void registerChain(DataProcessorChain chain) {
        if (chain == null || chain.getName() == null) {
            throw new IllegalArgumentException("处理器链名称不能为空");
        }

        // 设置处理器管理器
        chain.setDataProcessorManager(dataProcessorManager);

        // 初始化链
        chain.init();

        // 注册到管理器
        chains.put(chain.getName(), chain);

        log.info("注册处理器链: {}, 节点数: {}", chain.getName(), chain.getNodeCount());

        // 发布链注册事件
        eventPublisher.publishEvent(new ChainRegisteredEvent(this, chain.getName()));
    }

    /**
     * 注销处理器链
     *
     * @param chainName 处理器链名称
     */
    public void unregisterChain(String chainName) {
        if (DEFAULT_CHAIN_NAME.equals(chainName)) {
            log.warn("不能注销默认处理器链");
            return;
        }

        DataProcessorChain chain = chains.remove(chainName);
        if (chain != null) {
            // 停止链
            if (chain.isRunning()) {
                chain.stop();
            }

            log.info("注销处理器链: {}", chainName);

            // 发布链注销事件
            eventPublisher.publishEvent(new ChainUnregisteredEvent(this, chainName));
        }
    }

    /**
     * 更新处理器链
     *
     * @param chain 更新后的处理器链
     */
    public void updateChain(DataProcessorChain chain) {
        if (chain == null || chain.getName() == null) {
            throw new IllegalArgumentException("处理器链名称不能为空");
        }

        DataProcessorChain existingChain = chains.get(chain.getName());
        if (existingChain == null) {
            log.warn("处理器链不存在，将注册新链: {}", chain.getName());
            registerChain(chain);
            return;
        }

        // 停止现有链
        if (existingChain.isRunning()) {
            existingChain.stop();
        }

        // 更新链
        chain.setDataProcessorManager(dataProcessorManager);
        chain.init();

        chains.put(chain.getName(), chain);

        log.info("更新处理器链: {}", chain.getName());

        // 发布链更新事件
        eventPublisher.publishEvent(new ChainUpdatedEvent(this, chain.getName()));
    }

    /**
     * 获取处理器链
     *
     * @param chainName 处理器链名称
     * @return 处理器链
     */
    public DataProcessorChain getChain(String chainName) {
        return chains.get(chainName);
    }

    /**
     * 获取所有处理器链
     *
     * @return 处理器链映射
     */
    public Map<String, DataProcessorChain> getAllChains() {
        return new HashMap<>(chains);
    }

    /**
     * 获取处理器链统计信息
     *
     * @param chainName 处理器链名称
     * @return 统计信息
     */
    public Map<String, Object> getChainStatistics(String chainName) {
        DataProcessorChain chain = chains.get(chainName);
        if (chain == null) {
            return Collections.emptyMap();
        }

        return chain.getStatistics();
    }

    /**
     * 获取所有处理器链统计信息
     *
     * @return 统计信息映射
     */
    public Map<String, Map<String, Object>> getAllChainStatistics() {
        Map<String, Map<String, Object>> allStats = new HashMap<>();

        for (Map.Entry<String, DataProcessorChain> entry : chains.entrySet()) {
            allStats.put(entry.getKey(), entry.getValue().getStatistics());
        }

        return allStats;
    }

    /**
     * 启用/禁用处理器链
     *
     * @param chainName 处理器链名称
     * @param enabled   是否启用
     */
    public void setChainEnabled(String chainName, boolean enabled) {
        DataProcessorChain chain = chains.get(chainName);
        if (chain != null) {
            chain.setEnabled(enabled);
            log.info("处理器链 {}: {}", chainName, enabled ? "启用" : "禁用");
        }
    }

    /**
     * 获取默认处理器链名称
     *
     * @return 默认链名称
     */
    public String getDefaultChainName() {
        return DEFAULT_CHAIN_NAME;
    }

    /**
     * 检查处理器链是否存在
     *
     * @param chainName 处理器链名称
     * @return 是否存在
     */
    public boolean containsChain(String chainName) {
        return chains.containsKey(chainName);
    }

    /**
     * 获取启用状态的处理器链
     *
     * @return 启用状态的处理器链列表
     */
    public List<DataProcessorChain> getEnabledChains() {
        List<DataProcessorChain> enabledChains = new ArrayList<>();

        for (DataProcessorChain chain : chains.values()) {
            if (chain.isEnabled()) {
                enabledChains.add(chain);
            }
        }

        return enabledChains;
    }

    /**
     * 根据数据点选择处理器链
     * 可以根据数据点属性动态选择处理链
     *
     * @param point 数据点
     * @return 合适的处理器链名称
     */
    public String selectChainForPoint(DataPoint point) {
        if (point == null) {
            return DEFAULT_CHAIN_NAME;
        }

        // 示例逻辑：根据数据点类型选择不同的链
        String dataType = point.getDataType();
        if (dataType != null) {
            switch (dataType.toUpperCase()) {
                case "BOOLEAN":
                    return "fast"; // 布尔类型使用快速链
                case "STRING":
                    return "quality"; // 字符串类型使用质量检查链
                default:
                    return DEFAULT_CHAIN_NAME;
            }
        }

        return DEFAULT_CHAIN_NAME;
    }
}

/**
 * 处理器链执行事件
 */
@Data
class ChainExecutionEvent {
    private final Object source;
    private final String chainName;
    private final String pointId;
    private final boolean success;
    private final long executionTime;

    public ChainExecutionEvent(Object source, String chainName, String pointId,
                               boolean success, long executionTime) {
        this.source = source;
        this.chainName = chainName;
        this.pointId = pointId;
        this.success = success;
        this.executionTime = executionTime;
    }

    // getters...
}

/**
 * 处理器链注册事件
 */
@Data
class ChainRegisteredEvent {
    private final Object source;
    private final String chainName;

    public ChainRegisteredEvent(Object source, String chainName) {
        this.source = source;
        this.chainName = chainName;
    }

    // getters...
}

/**
 * 处理器链注销事件
 */
@Data
class ChainUnregisteredEvent {
    private final Object source;
    private final String chainName;

    public ChainUnregisteredEvent(Object source, String chainName) {
        this.source = source;
        this.chainName = chainName;
    }

    // getters...
}

/**
 * 处理器链更新事件
 */
@Data
class ChainUpdatedEvent {
    private final Object source;
    private final String chainName;

    public ChainUpdatedEvent(Object source, String chainName) {
        this.source = source;
        this.chainName = chainName;
    }

}