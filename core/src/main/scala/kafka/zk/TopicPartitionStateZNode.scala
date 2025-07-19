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
package kafka.zk

import scala.collection.mutable
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.{Json, Utils}
import scala.jdk.CollectionConverters._

/**
 * Class for managing ZooKeeper node for topic partition state.
 * The ZNode path is: /brokers/topics/[topic]/partitions/[partition]/state
 */
object TopicPartitionStateZNode {
  
  /**
   * Get the ZNode path for a specific topic partition state
   * @param topic Topic name
   * @param partition Partition ID
   * @return Full ZNode path to the partition state
   */
  def path(topic: String, partition: Int): String = {
    s"/brokers/topics/$topic/partitions/$partition/state"
  }
  
  /**
   * Get the ZNode path for a specific topic partition
   * @param tp TopicPartition object
   * @return Full ZNode path to the partition state
   */
  def path(tp: TopicPartition): String = {
    path(tp.topic, tp.partition)
  }
  
  /**
   * Encode partition state to JSON format for ZooKeeper
   * @param state Partition state as a map
   * @return JSON bytes for the state
   */
  def encode(state: Map[String, Any]): Array[Byte] = {
    val jsonMap = new java.util.HashMap[String, Any]()
    state.foreach { case (k, v) => jsonMap.put(k, v) }
    Json.encodeAsBytes(jsonMap)
  }
  
  /**
   * Decode partition state JSON from ZooKeeper
   * @param bytes JSON bytes from ZooKeeper
   * @param version ZNode version
   * @return Decoded partition state as a map
   */
  def decode(bytes: Array[Byte], version: Int): Map[String, Any] = {
    val json = Json.parseBytes(bytes)
    val result = mutable.Map.empty[String, Any]
    
    val jsonMap = json.asInstanceOf[java.util.Map[String, Any]]
    jsonMap.asScala.foreach { case (k, v) => 
      result.put(k, v match {
        case list: java.util.List[_] => list.asScala.toList
        case other => other
      })
    }
    
    result.put("version", version)
    result.toMap
  }
} 