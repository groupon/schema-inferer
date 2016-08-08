/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.groupon.dse.examples

import java.util.{Calendar, Properties}

import com.groupon.dse.configs.SchemaConfigBuilder
import com.groupon.dse.consumers.KafkaConsumerBuilder
import com.groupon.dse.extractor.{Extractor, RawBasicExtractor}
import com.groupon.dse.schema.publisher.hive.HivePublisherBuilder
import com.groupon.dse.schema.publisher.{DefaultPublisher, SchemaPublisher}
import com.groupon.dse.spark.plugins.ReceiverPlugin
import com.groupon.dse.streaming.plugins.SchemaInfererPlugin
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
 * Application that uses Schema Inferer library to infer the schema for a give Kafka topic
 */
object SampleSchemaInfererDriver {
  val CLASSNAME = "schemaInferer"
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    logger.info(s"Driver starting at ${Calendar.getInstance().getTimeInMillis}")
    val consumptionBatchIntervalInSec = 6
    //Load Configs from a file
    val appConfigs = loadConfigsFromFile("schema.properties")
    //val appConfigs = configs

    // Spark related initializations
    val sparkConf = new SparkConf().setAppName(CLASSNAME)
    val batchInterval = Seconds(consumptionBatchIntervalInSec)
    val streamingContext = new StreamingContext(sparkConf, batchInterval)

    //Build the Kafka receiver object
    val consumer = KafkaConsumerBuilder(streamingContext, appConfigs)

    //Schema inferer related initializations
    val schemaConfigs = SchemaConfigBuilder(appConfigs, streamingContext.sparkContext)
    val hivePublisher:SchemaPublisher = HivePublisherBuilder(appConfigs, streamingContext.sparkContext)
    val defaultPublisher:SchemaPublisher = new DefaultPublisher

    // Plugin initializations
    val loggerExtractor: Extractor = new RawBasicExtractor

    val schemaInferer: ReceiverPlugin = new SchemaInfererPlugin(new SQLContext(streamingContext.sparkContext),
      loggerExtractor,
      schemaConfigs,
      defaultPublisher)

    val plugins = Seq(schemaInferer)

    consumer.fetchFromKafka(plugins)

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def loadConfigsFromFile(fileName: String): Properties = {
    val properties = new Properties()
    val content = scala.io.Source.fromFile(fileName).bufferedReader()
    properties.load(content)
    properties
  }

  /**
   * List of configs required for the Schema Inferer application. Instead of hard-coding them, the configs
   * can also be loaded from a file provided at runtime
   */
  def appConfigs: Properties = {
    val properties = new Properties()

    // ##### Schema Inferer related configs #####

    // Set a TIME based warm up interval. A schema inferred for a topic will not be stored/published until this much
    // time in seconds has expired
    properties.setProperty("topic.warmup.policy", "TIME")
    properties.setProperty("topic.warmup.time.sec", "20")

    // Define the SchemaStore to be used. In this case use Zookeeper and provide the required configs
    properties.setProperty("store.type", "ZOOKEEPER")
    properties.setProperty("store.zk.connect", "localhost:2181")
    properties.setProperty("store.zk.root", "/sample_schema_inferer")

    // ##### Baryon related configs #####

    // List of topics for which a schema will be inferred
    properties.setProperty("topics", "test_topic")

    // Zookeeper endpoint used by Kafka brokers
    properties.setProperty("kafka.broker.zk.connect", "localhost:2181")

    // Point in the Kafka data stream to begin consumption from
    properties.setProperty("topic.start.offset", "-1")

    // Policy defining when a receiver should fetch data for a Kafka partition
    properties.setProperty("topic.consumption.policy", "OFFSET")

    // StateController required to interact with the state storage system (in this case Zookeeper)
    properties.setProperty("statecontroller.type", "ZOOKEEPER")
    properties.setProperty("statecontroller.zk.connect", "localhost:2181")
    properties.setProperty("statecontroller.zk.root", "/statecontroller/sample_schema_inferer")

    // Number of Spark Receivers to fetch Kafka data
    properties.setProperty("spark.num.receivers", "6")

    properties
  }
}