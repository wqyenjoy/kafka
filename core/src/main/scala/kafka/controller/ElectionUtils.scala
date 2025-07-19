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

import kafka.server.KafkaServer
import org.apache.kafka.common.TopicPartition
import scala.concurrent.{ExecutionContext, Future}

/**
 * Utility functions for working with leader elections
 */
object ElectionUtils {
  
  /**
   * Check if the topic partition should use atomic election transaction
   * 
   * @param server KafkaServer instance
   * @param topicPartition Topic partition to check
   * @return True if the partition should use atomic election transaction
   */
  def shouldUseElectionTx(server: KafkaServer, topicPartition: TopicPartition): Boolean = {
    server.getElectionTxnManager.isDefined && {
      // Use the election transaction if configured for this topic
      val config = server.electionTxnConfig
      config.isEnabledForTopic(topicPartition.topic)
    }
  }
  
  /**
   * Execute a leader election using the election transaction manager
   * This ensures that leader election and epoch increment are atomic.
   * 
   * @param server KafkaServer instance
   * @param topicPartition Topic partition to elect leader for
   * @param newLeader New leader broker ID
   * @param newIsr New ISR set
   * @return Future with the election result
   */
  def executeElectionTx(
    server: KafkaServer,
    topicPartition: TopicPartition,
    newLeader: Int,
    newIsr: Set[Int]
  )(implicit ec: ExecutionContext): Future[ElectionTxResult] = {
    val manager = server.getElectionTxnManager.getOrElse(
      throw new IllegalStateException("Election transaction manager not available")
    )
    
    val input = ElectionTxInput(
      tp = topicPartition,
      newLeader = newLeader,
      newIsr = newIsr
    )
    
    // Record the start time for latency measurement
    val startTimeMs = System.currentTimeMillis()
    
    // Execute the election transaction and measure latency
    val resultFuture = Future {
      val result = manager.commitElection(input)
      
      // Get metrics if available
      server.electionTxnMetrics.foreach { metrics =>
        val latencyMs = System.currentTimeMillis() - startTimeMs
        metrics.recordSuccess(latencyMs)
      }
      
      // Record leader and epoch for monitoring
      server.getLeaderEpochMonitor.foreach { monitor =>
        monitor.recordLeaderAndEpoch(topicPartition, newLeader, result.newEpoch)
      }
      
      result
    }.recover { case e =>
      // Record failure
      server.electionTxnMetrics.foreach(_.recordFailure())
      throw e
    }
    
    resultFuture
  }
} 