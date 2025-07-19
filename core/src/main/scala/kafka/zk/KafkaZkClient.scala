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

import kafka.utils.Logging
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{Op, OpResult, CreateMode, KeeperException}
import java.util.concurrent.CountDownLatch
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooKeeper.States
import scala.jdk.CollectionConverters._

/**
 * A ZooKeeper client that provides common operations needed by Kafka.
 */
class KafkaZkClient private (
  zkConnection: ZooKeeperConnection,
  isSecure: Boolean,
  time: Time
) extends Logging {

  import KafkaZkClient._

  private val syncTimeout = 30000
  
  /**
   * Get data and Stat of the specified ZNode
   * @param path Path of the ZNode
   * @return Tuple of (data bytes, stat)
   */
  def getDataAndStat(path: String): (Array[Byte], Stat) = {
    val stat = new Stat()
    val data = zkConnection.zooKeeper.getData(path, false, stat)
    (data, stat)
  }
  
  /**
   * Execute multiple ZooKeeper operations as a transaction
   * @param operations List of operations to execute atomically
   * @return List of operation results
   */
  def multi(operations: Seq[Op]): Seq[OpResult] = {
    if (operations.isEmpty)
      return Seq.empty
    
    try {
      zkConnection.zooKeeper.multi(operations.asJava).asScala.toSeq
    } catch {
      case e: KeeperException => 
        error(s"Error executing ZooKeeper multi-operations: ${e.getMessage}")
        throw e
    }
  }
  
  /**
   * Create a new ZNode with the given data
   * 
   * @param path Path of ZNode to create
   * @param data Data to store
   * @param createMode ZNode create mode (persistent, ephemeral, etc)
   * @param createParents Whether to create parent nodes if missing
   * @return Path of created ZNode
   */
  def createRecursive(path: String, data: Array[Byte], createMode: CreateMode, createParents: Boolean = true): String = {
    if (!createParents) {
      return zkConnection.zooKeeper.create(path, data, zkConnection.defaultAcls, createMode)
    }
    
    // Create parent nodes if necessary
    val parts = path.split("/").filter(_.nonEmpty)
    var currentPath = ""
    for (i <- 0 until parts.length - 1) {
      currentPath += "/" + parts(i)
      try {
        zkConnection.zooKeeper.create(currentPath, Array.empty[Byte], zkConnection.defaultAcls, CreateMode.PERSISTENT)
      } catch {
        case _: KeeperException.NodeExistsException => // Path already exists, continue
      }
    }
    
    // Create actual node
    currentPath += "/" + parts.last
    zkConnection.zooKeeper.create(currentPath, data, zkConnection.defaultAcls, createMode)
  }
  
  /**
   * Close the ZooKeeper client connection
   */
  def close(): Unit = {
    zkConnection.close()
  }
}

object KafkaZkClient {
  /**
   * Create a new KafkaZkClient instance
   * 
   * @param connectString ZooKeeper connection string
   * @param isSecure Whether to use secure ACLs
   * @param sessionTimeoutMs ZooKeeper session timeout
   * @param connectionTimeoutMs ZooKeeper connection timeout
   * @param maxInFlightRequests Maximum in-flight requests
   * @param time Time implementation
   * @param name Client name for logging
   * @param zkClientConfig Optional ZooKeeper client configuration
   * @return A new KafkaZkClient instance
   */
  def apply(
    connectString: String,
    isSecure: Boolean,
    sessionTimeoutMs: Int, 
    connectionTimeoutMs: Int,
    maxInFlightRequests: Int,
    time: Time,
    name: Option[String] = None,
    zkClientConfig: Option[ZKClientConfig] = None
  ): KafkaZkClient = {
    val zkConnection = new ZooKeeperConnection(
      connectString, 
      sessionTimeoutMs, 
      connectionTimeoutMs,
      maxInFlightRequests,
      time, 
      name, 
      zkClientConfig
    )
    new KafkaZkClient(zkConnection, isSecure, time)
  }
  
  /**
   * ZooKeeper client configuration
   * 
   * @param properties Configuration properties
   */
  case class ZKClientConfig(properties: Map[String, String] = Map.empty)
}

/**
 * Manages the ZooKeeper connection
 */
class ZooKeeperConnection(
  connectString: String,
  sessionTimeoutMs: Int,
  connectionTimeoutMs: Int,
  maxInFlightRequests: Int,
  time: Time,
  name: Option[String],
  clientConfig: Option[KafkaZkClient.ZKClientConfig]
) extends Logging {

  // Default ACLs for ZNodes
  val defaultAcls = ZkUtils.defaultAcls(false)
  
  private var _zkClient: ZooKeeper = null
  
  // Create the ZooKeeper client
  createZooKeeper()
  
  /**
   * Initialize the ZooKeeper connection
   */
  private def createZooKeeper(): Unit = {
    info(s"Connecting to ZooKeeper at $connectString")
    
    val connectionLatch = new CountDownLatch(1)
    val watcher = new ZKSessionWatcher(connectionLatch)
    
    _zkClient = new ZooKeeper(connectString, sessionTimeoutMs, watcher)
    
    // Wait for connection to be established
    if (!connectionLatch.await(connectionTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)) {
      close()
      throw new RuntimeException(s"Timed out waiting for connection to ZooKeeper: $connectString")
    }
    
    info(s"Connected to ZooKeeper at $connectString")
  }
  
  /**
   * Close the ZooKeeper connection
   */
  def close(): Unit = {
    info("Closing ZooKeeper connection")
    if (_zkClient != null) {
      _zkClient.close()
      _zkClient = null
    }
  }
  
  /**
   * Get the ZooKeeper client
   */
  def zooKeeper: ZooKeeper = {
    if (_zkClient == null || !_zkClient.getState.equals(States.CONNECTED))
      throw new IllegalStateException("ZooKeeper client is not connected")
    _zkClient
  }
  
  /**
   * ZooKeeper session watcher
   */
  class ZKSessionWatcher(connectionLatch: CountDownLatch) extends org.apache.zookeeper.Watcher {
    override def process(event: org.apache.zookeeper.WatchedEvent): Unit = {
      event.getState match {
        case KeeperState.SyncConnected => 
          connectionLatch.countDown()
        case KeeperState.Expired =>
          warn("ZooKeeper session expired")
        case KeeperState.Disconnected =>
          warn("ZooKeeper disconnected")
        case KeeperState.AuthFailed =>
          error("ZooKeeper authentication failed")
        case _ => // Do nothing for other states
      }
    }
  }
}

/**
 * ZooKeeper utility functions
 */
object ZkUtils {
  /**
   * Get default ACLs based on security settings
   * 
   * @param isSecure Whether to use secure ACLs
   * @return Default ACLs
   */
  def defaultAcls(isSecure: Boolean): java.util.ArrayList[org.apache.zookeeper.data.ACL] = {
    if (isSecure) {
      org.apache.zookeeper.ZooDefs.Ids.CREATOR_ALL_ACL
    } else {
      org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE
    }
  }
} 