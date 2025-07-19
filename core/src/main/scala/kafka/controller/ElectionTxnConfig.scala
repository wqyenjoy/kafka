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

import java.util.{Collections, Properties}
import kafka.server.KafkaConfig
import kafka.utils.{CoreUtils, Logging}
import scala.collection.mutable

/**
 * Configuration for the Election Transaction feature
 */
class ElectionTxnConfig(config: KafkaConfig) extends Logging {
  
  /**
   * Whether to use the Election Transaction feature
   * Only enabled if:
   * 1. The user has enabled it explicitly via configuration
   * 2. We're running in ZooKeeper mode (not KRaft)
   */
  val useElectionTx: Boolean = config.getBoolean(ElectionTxnConfig.UseElectionTxProp) && !isKRaftMode
  
  /**
   * Maximum number of retries for Election Transaction operations
   */
  val retryMax: Int = config.getInt(ElectionTxnConfig.ElectionTxRetryMaxProp)
  
  /**
   * Set of topic names for which Election Transaction is enabled.
   * If empty, applies to all topics when useElectionTx is true.
   */
  val enabledTopics: Set[String] = {
    val topicList = config.getList(ElectionTxnConfig.ElectionTxEnabledTopicsProp)
    if (topicList.isEmpty) {
      Collections.emptySet[String]()
    } else {
      topicList.toSet
    }
  }
  
  /**
   * Determine if Election Transaction should be used for a specific topic
   */
  def isEnabledForTopic(topic: String): Boolean = {
    useElectionTx && (enabledTopics.isEmpty || enabledTopics.contains(topic))
  }
  
  /**
   * Determine if we're running in KRaft mode
   */
  private def isKRaftMode: Boolean = {
    // Check for process.roles which would indicate KRaft mode
    val processRoles = config.getString("process.roles", "")
    processRoles.nonEmpty
  }
  
  override def toString: String = {
    s"ElectionTxnConfig(useElectionTx=$useElectionTx, retryMax=$retryMax, " +
      s"enabledTopics=${if (enabledTopics.isEmpty) "all" else enabledTopics.mkString(",")}, " +
      s"isKRaftMode=${isKRaftMode})"
  }
}

object ElectionTxnConfig {
  // Configuration property names
  val UseElectionTxProp = "controller.use.election.tx"
  val ElectionTxRetryMaxProp = "controller.election.tx.retry.max"
  val ElectionTxEnabledTopicsProp = "controller.election.tx.enabled.topics"
  
  // Default values
  val DefaultUseElectionTx = false
  val DefaultElectionTxRetryMax = 3
  val DefaultElectionTxEnabledTopics = ""
  
  /**
   * Add election transaction configuration defaults to Kafka config
   */
  def addToConfigDef(): Unit = {
    import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
    
    KafkaConfig.configDef
      .define(UseElectionTxProp, Type.BOOLEAN, DefaultUseElectionTx, Importance.MEDIUM,
        "Whether to use atomic election transactions for leader changes in ZooKeeper mode (ignored in KRaft mode).")
      .define(ElectionTxRetryMaxProp, Type.INT, DefaultElectionTxRetryMax, Importance.MEDIUM,
        "Maximum number of retry attempts for election transactions.")
      .define(ElectionTxEnabledTopicsProp, Type.LIST, DefaultElectionTxEnabledTopics, Importance.MEDIUM,
        "List of topics for which election transactions are enabled. If empty, enabled for all topics if controller.use.election.tx=true.")
  }
} 