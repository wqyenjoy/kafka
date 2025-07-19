import kafka.controller.{ElectionTxnConfig, ElectionTxnManager, ElectionTxnManagerZk, ElectionTxnMetrics}

class KafkaServer(
  val config: KafkaConfig,
  // ... existing code ...
) {
  // ... existing code ...
  
  private val _electionTxnConfig = new ElectionTxnConfig(config)
  
  /**
   * Get the ElectionTxnConfig
   */
  def electionTxnConfig: ElectionTxnConfig = _electionTxnConfig
  private var electionTxnMetrics: Option[ElectionTxnMetrics] = None
  private var electionTxnManager: Option[ElectionTxnManager] = None
  private var leaderEpochMonitor: Option[LeaderEpochMonitor] = None
  
  // ... existing code ...

    /**
   * Get the ElectionTxnManager if available
   */
  def getElectionTxnManager: Option[ElectionTxnManager] = {
    electionTxnManager
  }
  
  /**
   * Get the LeaderEpochMonitor if available
   */
  def getLeaderEpochMonitor: Option[LeaderEpochMonitor] = {
    leaderEpochMonitor
  }

  private def createZkClient(time: Time): KafkaZkClient = {
    info(s"Connecting to ZooKeeper on ${config.zkConnect}")
    
    val zkClientConfig = Option(new KafkaZkClient.ZKClientConfig())
    val zkClient = KafkaZkClient(
      config.zkConnect,
      config.zkEnableSecureAcls,
      config.zkSessionTimeoutMs,
      config.zkConnectionTimeoutMs,
      config.zkMaxInFlightRequests,
      time,
      name = Some("Kafka server"),
      zkClientConfig = zkClientConfig)
    
    // Initialize ElectionTxnManager if the feature is enabled and we're not in KRaft mode
    if (_electionTxnConfig.useElectionTx) {
      // In KRaft mode, this will already be false due to the check in ElectionTxnConfig
      info(s"Initializing Election Transaction Manager with config: $_electionTxnConfig")
      val metrics = new ElectionTxnMetrics()
      val manager = new ElectionTxnManagerZk(zkClient, _electionTxnConfig.retryMax)
      val monitor = new LeaderEpochMonitor(metrics)
      
      electionTxnMetrics = Some(metrics)
      electionTxnManager = Some(manager)
      leaderEpochMonitor = Some(monitor)
      
      info("Election Transaction Manager initialized successfully")
    } else {
      // Check if this feature was explicitly enabled but we're in KRaft mode
      val processRoles = config.getString("process.roles", "")
      val featureExplicitlyEnabled = config.originals().containsKey(ElectionTxnConfig.UseElectionTxProp) && 
                                     config.getBoolean(ElectionTxnConfig.UseElectionTxProp)
      
      if (featureExplicitlyEnabled && processRoles.nonEmpty) {
        warn("Election Transaction feature was explicitly enabled but is being ignored because KRaft mode is active. " +
             "This feature is only applicable in ZooKeeper mode.")
      }
    }
    
    zkClient
  }
  
  // ... existing code ...

  /**
   * Shutdown the server
   */
  def shutdown(): Unit = {
    // ... existing code ...
    
    // Close leader epoch monitor if initialized
    leaderEpochMonitor.foreach { monitor =>
      info("Resetting Leader Epoch Monitor")
      monitor.reset()
    }
    
    // Close election transaction metrics if initialized
    electionTxnMetrics.foreach { metrics =>
      info("Closing Election Transaction Metrics")
      metrics.close() 
    }
    
    // ... existing code ...
  }
  
  // ... existing code ...
}

// ... existing code ...

object KafkaServer {
  // ... existing code ...
  
  def initializeElectionTxnConfig(): Unit = {
    info("Initializing Election Transaction Configuration")
    ElectionTxnConfig.addToConfigDef()
  }
  
  // ... existing code ...
} 