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

import org.apache.kafka.common.TopicPartition

/**
 * Input data for an election transaction.
 *
 * @param tp          Topic partition
 * @param newLeader   New leader broker ID
 * @param newIsr      New ISR set
 */
case class ElectionTxInput(
  tp: TopicPartition,
  newLeader: Int,
  newIsr: Set[Int]
)

/**
 * Result of an election transaction.
 *
 * @param newEpoch    Updated leader epoch after the transaction
 * @param zkVersion   ZooKeeper version after the transaction (used for idempotency)
 */
case class ElectionTxResult(
  newEpoch: Int,
  zkVersion: Int
)

/**
 * Interface for managing atomic leader election transactions.
 * This ensures that leader changes and leader epoch increments are performed atomically.
 */
trait ElectionTxnManager {
  /**
   * Commits an election transaction that atomically updates the leader, ISR and leader epoch.
   *
   * @param in The election input parameters
   * @return The result of the election transaction
   */
  def commitElection(in: ElectionTxInput): ElectionTxResult
} 