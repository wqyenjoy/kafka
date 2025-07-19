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
import com.yammer.metrics.core.{Gauge, Meter}
import kafka.metrics.KafkaMetricsGroup
import java.util.concurrent.TimeUnit

/**
 * Metrics related to election transactions
 */
class ElectionTxnMetrics extends KafkaMetricsGroup with Logging {
  
  // Success rate metrics
  private val successMeter = newMeter("ElectionTransactionSuccessRate", "elections", TimeUnit.SECONDS)
  private val failureMeter = newMeter("ElectionTransactionFailureRate", "failures", TimeUnit.SECONDS)
  
  // Latency metrics
  private val latencySensor = newHistogram("ElectionTransactionLatencyMs")

  // Count metrics
  private val retryCounter = newCounter("ElectionTransactionRetryCount")
  
  // Violation metrics
  private val epochViolationCounter = newCounter("LeaderEpochViolationCount")
  
  /**
   * Record a successful election transaction
   * 
   * @param latencyMs The latency of the transaction in milliseconds
   */
  def recordSuccess(latencyMs: Long): Unit = {
    successMeter.mark()
    latencySensor.update(latencyMs)
  }
  
  /**
   * Record a failed election transaction
   */
  def recordFailure(): Unit = {
    failureMeter.mark()
  }
  
  /**
   * Record a retry attempt
   */
  def recordRetry(): Unit = {
    retryCounter.inc()
  }
  
  /**
   * Record a leader epoch violation (leader changed but epoch didn't increase)
   */
  def recordEpochViolation(): Unit = {
    epochViolationCounter.inc()
    error("CRITICAL: Detected leader epoch violation! Leader changed but epoch was not incremented")
  }
  
  /**
   * Close and unregister all metrics
   */
  def close(): Unit = {
    removeMetric("ElectionTransactionSuccessRate")
    removeMetric("ElectionTransactionFailureRate")
    removeMetric("ElectionTransactionLatencyMs")
    removeMetric("ElectionTransactionRetryCount") 
    removeMetric("LeaderEpochViolationCount")
  }
} 