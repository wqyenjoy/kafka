# KAFKA-19508: Producer keeps reporting a NotLeaderOrFollowerException

## 问题描述

在服务器重启后，生产者持续报告 `NotLeaderOrFollowerException` 异常。经调查发现，根本原因是leader发生变化但leaderEpoch没有更新，导致了"僵尸leader"现象。

## 解决方案概述

我们实现了一个基于ZooKeeper的选举事务（Election TX）设计，确保leader变更和leaderEpoch递增是原子操作。该设计保证这两个操作要么一起成功，要么一起失败，从而消除了leader和epoch不同步的可能性。

### 核心组件

1. **ElectionTxnManager**: 
   - 管理选举事务的接口
   - `ElectionTxnManagerZk`: ZooKeeper实现，使用ZK的multi()操作原子地更新leader和epoch

2. **LeaderEpochMonitor**: 
   - 监视leader和epoch变化
   - 检测并报告违例情况（如leader变更但epoch未递增）

3. **ElectionTxnMetrics**:
   - 提供关键指标监控：成功率、失败率、延迟等
   - 记录epoch违例情况

4. **ElectionTxnConfig**:
   - 控制特性开关和行为
   - 支持按主题启用/禁用，便于灰度发布

### 关键改动

1. `KafkaZkClient`: 封装ZooKeeper操作，提供multi()原子操作支持
2. `TopicPartitionStateZNode`: 处理ZooKeeper节点数据的编解码
3. `ElectionUtils`: 提供实用方法判断和执行选举事务

## 配置参数

```properties
# 是否启用选举事务功能（默认：false）
controller.use.election.tx=true

# 最大重试次数（默认：3）
controller.election.tx.retry.max=3

# 启用选举事务的主题列表（逗号分隔，默认为空表示全部启用）
controller.election.tx.enabled.topics=topic1,topic2
```

## 监控指标

- `ElectionTransactionSuccessRate`: 每秒成功事务数
- `ElectionTransactionFailureRate`: 每秒失败事务数
- `ElectionTransactionLatencyMs`: 事务延迟（毫秒）
- `ElectionTransactionRetryCount`: 重试次数
- `LeaderEpochViolationCount`: epoch违例计数（关键指标，应为0）

## 部署建议

1. **灰度发布**: 
   - 先升级二进制但保持开关关闭
   - 选择低流量主题进行测试（使用`controller.election.tx.enabled.topics`）
   - 逐步扩大范围，确认稳定后全局启用

2. **回滚策略**:
   - 发现问题时，修改配置`controller.use.election.tx=false`
   - 等待配置热加载或重启控制器节点

## 效果

实施此方案后，将彻底解决僵尸leader问题，确保leader变更和epoch递增的原子性，消除生产者报告的NotLeaderOrFollowerException异常。 