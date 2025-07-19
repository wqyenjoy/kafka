/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.controller

import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.ConcurrentHashMap

/**
 * Monitors leader epoch changes to detect violations
 * A violation occurs when a leader changes but the epoch doesn't increment.
 */
class LeaderEpochMonitor(metrics: ElectionTxnMetrics) extends Logging {
  
  // Map of partition to its current leader and epoch
  private val partitionLeaderEpochs = new ConcurrentHashMap[TopicPartition, PartitionLeaderEpochState]()
  
  /**
   * Record the current leader and epoch for a partition.
   * This will check for violations where the leader changes but epoch doesn't.
   * 
   * @param partition The topic partition
   * @param leader The current leader broker ID
   * @param epoch The current epoch
   */
  def recordLeaderAndEpoch(partition: TopicPartition, leader: Int, epoch: Int): Unit = {
    val previous = partitionLeaderEpochs.get(partition)
    
    if (previous != null) {
      if (previous.leader != leader && previous.epoch == epoch) {
        // Detected violation: leader changed but epoch didn't increment
        metrics.recordEpochViolation()
        error(s"LEADER EPOCH VIOLATION DETECTED: Partition $partition changed leader " +
          s"from ${previous.leader} to $leader but epoch remained at $epoch")
      }
    }
    
    partitionLeaderEpochs.put(partition, PartitionLeaderEpochState(leader, epoch))
  }
  
  /**
   * Reset the monitor state, typically used when shutting down or resetting the controller
   */
  def reset(): Unit = {
    partitionLeaderEpochs.clear()
  }
  
  /**
   * Get the current state for a partition
   * 
   * @param partition The topic partition
   * @return The current state or None if no state exists
   */
  def getState(partition: TopicPartition): Option[PartitionLeaderEpochState] = {
    Option(partitionLeaderEpochs.get(partition))
  }
}

/**
 * State of a partition's leader and epoch
 * 
 * @param leader Current leader broker ID
 * @param epoch Current leader epoch
 */
case class PartitionLeaderEpochState(leader: Int, epoch: Int) 