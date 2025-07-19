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
import kafka.zk.{KafkaZkClient, TopicPartitionStateZNode}
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.protocol.Errors
import org.apache.zookeeper.KeeperException.{BadVersionException, NodeExistsException}
import org.apache.zookeeper.data.Stat

import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._
import org.apache.zookeeper.{CreateMode, KeeperException, Op}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

/**
 * ZooKeeper implementation of ElectionTxnManager.
 * This implementation uses ZK multi() operation to atomically update leader, ISR and leader epoch.
 */
class ElectionTxnManagerZk(zkClient: KafkaZkClient, retryMax: Int = 3) extends ElectionTxnManager with Logging {

  /**
   * Commits an election transaction that atomically updates the leader, ISR and leader epoch.
   *
   * @param in The election input parameters
   * @return The result of the election transaction
   */
  override def commitElection(in: ElectionTxInput): ElectionTxResult = {
    val tp = in.tp
    val statePath = TopicPartitionStateZNode.path(tp.topic, tp.partition)
    
    var retries = 0
    var lastException: Throwable = null

    while (retries < retryMax) {
      try {
        // First read the current state to get the current leader epoch
        val (currJsonBytes, stat) = zkClient.getDataAndStat(statePath)
        val currState = TopicPartitionStateZNode.decode(currJsonBytes, stat.getVersion)
        
        // Create the new state with incremented leader epoch
        val newEpoch = currState.leaderEpoch + 1
        val newState = currState.copy(
          leader = in.newLeader,
          leaderEpoch = newEpoch,
          isr = in.newIsr.toSeq.sorted
        )
        
        // Create new JSON with updated state
        val newJsonBytes = TopicPartitionStateZNode.encode(newState)
        
        // Execute the ZK multi operation
        zkClient.multi(Seq(
          Op.check(statePath, stat.getVersion), // Optimistic locking
          Op.setData(statePath, newJsonBytes, stat.getVersion) // Actual update
        ))
        
        return ElectionTxResult(newEpoch, stat.getVersion + 1)
      } catch {
        case e: BadVersionException =>
          // Version conflict, someone else modified the ZNode, retry with fresh state
          info(s"Version conflict when updating leader for $tp, retrying (${retries+1}/$retryMax)")
          retries += 1
          lastException = e
          
        case e: ControllerMovedException =>
          // Controller changed, abort operation
          error(s"Controller moved during leader election for $tp")
          throw e
          
        case e: Exception =>
          // Unexpected error
          error(s"Unexpected error in commitElection for $tp", e)
          throw e
      }
    }
    
    val errorMsg = s"Failed to commit election for $tp after $retryMax retries"
    error(errorMsg, lastException)
    throw new IllegalStateException(errorMsg, lastException)
  }
} 