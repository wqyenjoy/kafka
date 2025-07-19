/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka

import java.util
import java.util.Properties

import kafka.metrics.KafkaMetricsReporter
import kafka.server.{KafkaConfig, KafkaServer, Server}
import kafka.utils._
import org.apache.kafka.common.utils.{Exit, Java, LoggingSignalHandler, OperatingSystem, Time, Utils}
import org.apache.kafka.server.config.ZooKeeperConfig
import org.apache.kafka.server.util.{CommandDefaultOptions, CommandLineUtils}

import scala.jdk.CollectionConverters._

/**
 * Starts a Kafka broker as a daemon.
 */
object Kafka extends Logging {

  def getPropsFromArgs(args: Array[String]): Properties = {
    val optionParser = new CommandDefaultOptions(args)
    if (args.length == 0 || optionParser.options.has(optionParser.helpOpt)) {
      CommandLineUtils.printUsageAndDie(optionParser.parser, "USAGE: java [options] %s server.properties [--override property=value]*".format(classOf[KafkaServer].getSimpleName))
    }

    if (optionParser.options.nonOptionArguments().size < 1) {
      CommandLineUtils.printUsageAndDie(optionParser.parser, "USAGE: java [options] %s server.properties [--override property=value]*".format(classOf[KafkaServer].getSimpleName))
    }
    // parse first argument as a path to properties file
    val serverProps = Utils.loadProps(optionParser.options.nonOptionArguments().get(0))

    // parse overwrites to the properties file
    if (optionParser.options.has(optionParser.overrideOpt) && !optionParser.options.valuesOf(optionParser.overrideOpt).isEmpty) {
      val overrides = optionParser.options.valuesOf(optionParser.overrideOpt).asScala.map(CommandLineUtils.parseKeyValue)

      CommandLineUtils.checkRequiredArgs(optionParser.parser, overrides, CommandLineUtils.KeyValueSeparator)

      val overrideProps = new Properties()
      overrides.foreach { case (k, v) => overrideProps.put(k, v) }
      serverProps.putAll(overrideProps)
    }
    serverProps
  }

  def main(args: Array[String]): Unit = {
    // Register election transaction config
    kafka.server.KafkaServer.initializeElectionTxnConfig()
    
    try {
      val serverProps = getPropsFromArgs(args)
      val statusFilePath = serverProps.getProperty(KafkaConfig.KafkaPidFileProp)
      val kafkaServerClass = serverProps.getProperty(KafkaConfig.KafkaServerClassProp, KafkaConfig.DefaultKafkaServerClass)

      val statusFile = Option(statusFilePath).map(new StatusFile(_, "pid"))

      // load server services classes
      val server = Class.forName(kafkaServerClass).getDeclaredConstructor().newInstance().asInstanceOf[Server]

      val exitCode = try {
        try {
          if (statusFile.isDefined)
            statusFile.foreach(_.write(kafka.utils.JVMInfoUtils.jvmIdString()))

          // attach shutdown handler to catch terminating signals as well as normal termination
          Exit.addShutdownHook("kafka-shutdown-hook", () => server.shutdown())
          LoggingSignalHandler.register(signals)

          server.startup()

          server.awaitShutdown()
          0
        }
        catch {
          case e: Throwable =>
            fatal("Exiting Kafka due to fatal exception", e)
            1
        }
      }
      finally {
        Exit.deleteHook("kafka-shutdown-hook")
        if (statusFile.isDefined)
          statusFile.foreach(_.delete())
      }
      Exit.exit(exitCode)
    }
    catch {
      case e: Throwable =>
        fatal("Exiting Kafka due to fatal exception during startup", e)
        Exit.exit(1)
    }
  }

  private def signals: Array[Signal] = {
    val handlers = new Array[Signal](3)
    handlers(0) = new Signal("INT")
    // SIGTERM is used for container orchestration and termination
    handlers(1) = new Signal("TERM")
    handlers(2) = OperatingSystem.get().getTerminationSignal
    handlers
  }
}
