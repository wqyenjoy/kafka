# KAFKA-19508: Producer keeps reporting NotLeaderOrFollowerException

## Problem Statement

After a broker restart, producers may repeatedly receive `NotLeaderOrFollowerException`.
The root cause is that the partition leader changed but the leaderEpoch did not advance, which creates a "zombie leader" situation.

## Important Notes

This mitigation only applies to ZooKeeper-based Kafka clusters.
It is automatically disabled under KRaft mode because KRaft uses a different controller and election mechanism. KRaft already guarantees atomic leader changes and epoch bumps via the Raft consensus protocol.

## Solution Overview

We introduce a ZooKeeper-based Election Transaction (Election TX) design to make the leader change and the leaderEpoch bump an atomic operation. The transaction ensures that these two updates either succeed together or fail together, removing the possibility of leader/epoch divergence.

### Core Components

1. ElectionTxnManager
   - Interface that manages the election transaction lifecycle
   - ElectionTxnManagerZk: ZooKeeper-based implementation that uses ZK multi() to atomically update the leader and epoch

2. LeaderEpochMonitor
   - Observes leader and epoch changes
   - Detects and reports violations (e.g., leader changed without an epoch bump)

3. ElectionTxnMetrics
   - Exposes key metrics: success rate, failure rate, latency, retry count
   - Records epoch violation occurrences

4. ElectionTxnConfig
   - Feature flags and behavior controls
   - Per-topic enablement for safe, gradual rollout
   - Auto-detection and disablement under KRaft mode

### Key Changes

1. KafkaZkClient: Wraps ZooKeeper operations and provides atomic multi() support
2. TopicPartitionStateZNode: Encodes/decodes partition state data in ZooKeeper
3. ElectionUtils: Utility helpers for validation and execution of election transactions

## Configuration

```properties
# Enable the election transaction feature (default: false). Ignored in KRaft mode.
controller.use.election.tx=true

# Maximum retry attempts for a transaction (default: 3)
controller.election.tx.retry.max=3

# Comma-separated list of topics to enable the feature for.
# Empty means enable for all topics (default: empty)
controller.election.tx.enabled.topics=topic1,topic2
```

## Metrics

- ElectionTransactionSuccessRate: Successful transactions per second
- ElectionTransactionFailureRate: Failed transactions per second
- ElectionTransactionLatencyMs: Transaction latency in milliseconds
- ElectionTransactionRetryCount: Number of retries performed
- LeaderEpochViolationCount: Count of epoch violations (should be 0)

## Rollout Recommendations

1. Gradual rollout
   - Upgrade binaries with the feature disabled
   - Test on low-traffic topics using `controller.election.tx.enabled.topics`
   - Expand coverage progressively and enable globally only after stability is verified

2. Rollback plan
   - If issues are observed, set `controller.use.election.tx=false`
   - Wait for dynamic config reload or restart controller nodes

## Expected Outcome

This solution eliminates the zombie leader scenario by guaranteeing the atomicity of leader changes and leaderEpoch bumps, thereby removing repeated NotLeaderOrFollowerException on producers.

## KRaft Compatibility

When KRaft mode is detected, the feature is automatically disabled and a warning is logged. KRaft inherently ensures leader/epoch consistency via its internal protocol.