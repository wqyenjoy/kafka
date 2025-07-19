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

import kafka.zk.{KafkaZkClient, TopicPartitionStateZNode}
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.KeeperException.BadVersionException
import org.apache.zookeeper.data.Stat
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import scala.collection.mutable
import org.apache.zookeeper.{Op, ZooDefs}
import org.junit.jupiter.api.Assertions._

class ElectionTxnManagerTest {

  private val zkClient = Mockito.mock(classOf[KafkaZkClient])
  private val testTopic = "test-topic"
  private val testPartition = 0
  private val tp = new TopicPartition(testTopic, testPartition)
  private val statePath = TopicPartitionStateZNode.path(testTopic, testPartition)
  private val testStat = new Stat()

  private val initialLeaderAndIsr = new LeaderAndIsrInfo(
    leader = 1,
    leaderEpoch = 5,
    isr = List(1, 2, 3),
    zkVersion = 10
  )

  @BeforeEach
  def setup(): Unit = {
    Mockito.reset(zkClient)
    testStat.setVersion(initialLeaderAndIsr.zkVersion)
  }

  private case class LeaderAndIsrInfo(
    leader: Int,
    leaderEpoch: Int,
    isr: List[Int],
    zkVersion: Int
  ) {
    def toJsonBytes: Array[Byte] = {
      val state = Map(
        "leader" -> leader,
        "leader_epoch" -> leaderEpoch,
        "isr" -> isr
      )
      TopicPartitionStateZNode.encode(state)
    }
  }

  @Test
  def testSuccessfulCommitElection(): Unit = {
    // Set up the mock to return the initial state
    val mockBytes = initialLeaderAndIsr.toJsonBytes
    when(zkClient.getDataAndStat(statePath)).thenReturn((mockBytes, testStat))
    
    // Mock the multi operation to be successful
    when(zkClient.multi(any())).thenReturn(null)

    val manager = new ElectionTxnManagerZk(zkClient)
    val input = ElectionTxInput(tp, 2, Set(1, 2, 3))
    val result = manager.commitElection(input)

    // Verify the result has incremented epoch
    assertEquals(initialLeaderAndIsr.leaderEpoch + 1, result.newEpoch)
    assertEquals(initialLeaderAndIsr.zkVersion + 1, result.zkVersion)

    // Verify the ZK client calls
    verify(zkClient).getDataAndStat(statePath)
    verify(zkClient).multi(any())
  }

  @Test
  def testVersionConflictRetries(): Unit = {
    // First call returns normal data
    val mockBytes = initialLeaderAndIsr.toJsonBytes
    when(zkClient.getDataAndStat(statePath)).thenReturn((mockBytes, testStat))
    
    // First multi call throws BadVersionException
    when(zkClient.multi(any()))
      .thenThrow(new BadVersionException())
      .thenReturn(null) // Second call succeeds

    val manager = new ElectionTxnManagerZk(zkClient)
    val input = ElectionTxInput(tp, 2, Set(1, 2, 3))
    val result = manager.commitElection(input)

    // Verify we got the expected result
    assertEquals(initialLeaderAndIsr.leaderEpoch + 1, result.newEpoch)
    assertEquals(initialLeaderAndIsr.zkVersion + 1, result.zkVersion)

    // Verify the ZK client calls (2 reads, 2 multi attempts)
    verify(zkClient, times(2)).getDataAndStat(statePath)
    verify(zkClient, times(2)).multi(any())
  }

  @Test
  def testMaxRetriesExceeded(): Unit = {
    // Always return valid data
    val mockBytes = initialLeaderAndIsr.toJsonBytes
    when(zkClient.getDataAndStat(statePath)).thenReturn((mockBytes, testStat))
    
    // Always throw BadVersionException
    when(zkClient.multi(any())).thenThrow(new BadVersionException())

    val manager = new ElectionTxnManagerZk(zkClient, retryMax = 3)
    val input = ElectionTxInput(tp, 2, Set(1, 2, 3))
    
    // Should throw exception after retryMax attempts
    assertThrows(classOf[IllegalStateException], () => manager.commitElection(input))

    // Verify the ZK client calls (3 attempts + 1 initial = 4 reads, 3 multi attempts)
    verify(zkClient, times(4)).getDataAndStat(statePath)
    verify(zkClient, times(3)).multi(any())
  }
} 