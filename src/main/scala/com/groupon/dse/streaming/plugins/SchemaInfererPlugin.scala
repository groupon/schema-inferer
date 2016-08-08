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

package com.groupon.dse.streaming.plugins

import java.util.Calendar

import com.groupon.dse.commons.{SchemaNormalizer, SchemaPrettyPrinter, SchemaState}
import com.groupon.dse.configs.SchemaInfererConfigs
import com.groupon.dse.extractor.Extractor
import com.groupon.dse.kafka.common.WrappedMessage
import com.groupon.dse.schema.Schema
import com.groupon.dse.schema.publisher.{Result, SchemaPublisher}
import com.groupon.dse.schema.store.FetchException
import com.groupon.dse.spark.plugins.ReceiverPlugin
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap

/**
 * Baryon Plugin to infer the schema of a data stream and publish/store it based on the user requirements.
 * @param sqlContext The spark sql context
 * @param extractor The extractor to use
 * @param configs The configs object having the user defined configs
 * @param publishers The list of publishers
 */
class SchemaInfererPlugin(sqlContext: SQLContext,
                    extractor: Extractor,
                    configs: SchemaInfererConfigs,
                    publishers: Seq[SchemaPublisher]) extends ReceiverPlugin with Serializable{

  private var initializedPublishers = false
  private val logger = LoggerFactory.getLogger(getClass)
  //Map that stores the state of the topic ie (Iterations, TimeLLastAccessed)
  private val topicStateMap = new scala.collection.mutable.HashMap[String, SchemaState].withDefaultValue(SchemaState(Calendar.getInstance().getTimeInMillis))
  //Map that decides if the publisher can publish schema changes for the topic
  private val topicStartMap = new scala.collection.mutable.HashMap[String, Boolean].withDefaultValue(false)
  private val schemaStore = configs.store
  private val startPolicy = configs.startPolicy
  private val samplingRatio = configs.samplingRatio.toDouble

  /**
   * Secondary constructor that takes only 1 publisher
   * @param sqlContext The spark sql context
   * @param extractor The extractor to use
   * @param configs The configs object having the user defined configs
   * @param publisher The schema publishers
   */
  def this(sqlContext: SQLContext,
           extractor: Extractor,
           configs: SchemaInfererConfigs,
           publisher: SchemaPublisher) {
    this(sqlContext, extractor, configs, Seq(publisher))
  }
  /**
   * The plugin executor which accepts RDD of Wrapped Messaged and processes them based on the requirements.
   * @param messages the RDD of wrapped messages
   */
  override def execute(messages: RDD[WrappedMessage]): Unit = {
    //Check for publisher initialization
    if(!initializedPublishers){
      initPublishers()
      initializedPublishers = true
    }
    val topicEventRDD = extractor.extract(messages.sample(false, samplingRatio)).cache()
    val topics = topicEventRDD.map(te => (te.topic, null)).reduceByKeyLocally((t1, t2) => t1).keys
    topics.foreach(topic => {
      val eventsForTopic = topicEventRDD.filter(te => te.topic == topic).map(te => te.event)
      try{
        val schemaRDD = sqlContext.read.json(eventsForTopic)
        val schema = schemaRDD.schema
        processSchema(schema, topic)
      }catch {
        //This exception is thrown when the strRDD is empty due to malformed jsons. It would throw an exception saying the RDD is empty.
        case e: UnsupportedOperationException =>
          logger.error(s"Malformed json. Skipping this batch of messages : ${e.getStackTraceString}")
      }
    })
    //Flush all dirty schemas to the store
    schemaStore.flush
  }

  /**
   * Fetches the schema from the store
   * @param topic The topic of the schema
   * @return The schema if present, null otherwise
   */
  private def getSchema(topic: String) : Schema = {
    try{
      schemaStore.get(topic).orNull
    }catch{
      case e : FetchException => {
        logger.error(s"[$topic] Error in retrieving schema from the store, restarting schema inference")
        null
      }
    }
  }

  /**
   * Processes the spark schema, stores it in store and publishes to the user defined location [Hive, Kafka, File etc]
   * @param schema The schema inferred by spark
   * @param topic The sourcetype of the schema
   */
  private def processSchema(schema: StructType, topic: String): Unit = {
    //Convert spark schema to Schema object
    val newSchema = SchemaNormalizer.normalize(schema, topic)

    logger.debug(s"[$topic] New schema : ${SchemaPrettyPrinter.print(newSchema.content)}")

    //Extract saved schema for the topic
    val savedSchema = getSchema(topic)

    //Compare the saved and new schema
    val diff = newSchema.diff(savedSchema)
    if(diff != null) {
      //Merge if there is a difference
      val mergedSchema = newSchema.union(savedSchema)

      if(topicStartMap(topic)){
        publishers.foreach(publisher => publishSchema(mergedSchema, diff, publisher)) //Not doing anything with the result. Failures should be handled by the publisher. Same applies to the schema store.
      }else {
        logger.debug(s"[$topic] Cold Start Period. Cannot publish as yet ${topicStateMap(topic).timestamp}")
      }

      //Store the merged schema to the schema store irrespective of the publisher outcome
      if(schemaStore.put(mergedSchema)) {
        logger.info(s"[${mergedSchema.topic}] Schema has changed with new fields ${SchemaPrettyPrinter.print(mergedSchema.parentDiff)} \n")
      } else {
        logger.error(s"[$topic] Error storing schema in store, skipping")
      }

    }else {
      logger.info(s"[$topic] All Keys present in the stored schema. Skipping the new schema ")
    }

    //Update the map that checks if the start policy has been met only if the publisher has not been started yet
    if(!topicStartMap(topic)) {
      topicStartMap(topic) = checkStartPolicy(topic)
    }

  }

  /**
   * Checks if the publisher start policy for the topic has been met. Publishes the schema for the first time if it exists in the store but has not beed published yet
   * @param topic topic of the schema
   * @return true if topic can be published, false otherwise
   */
  private def checkStartPolicy(topic: String): Boolean = {
    val schemaState = SchemaState(Calendar.getInstance().getTimeInMillis)
    topicStateMap(topic) = schemaState
    if(startPolicy.canStart(schemaState)){
      val schema = getSchema(topic)
      if(schema != null) {
        logger.info(s"[$topic] Starting publishers for the topic at Time  : ${topicStateMap(topic).timestamp}]. Publishing first schema")
        publishers.foreach(publisher => publishSchema(schema, schema.content, publisher))
      }
      return true
    }
    false
  }

  /**
   * Publishes the schema
   * @param schema The schema to publish
   * @return True if publish is successful, false otherwise
   */
  private def publishSchema(schema: Schema, diff: HashMap[String, Any], publisher: SchemaPublisher): Boolean = {
    if(!publisher.isInitialized){
      logger.error(s"[${schema.topic}] Not publishing schema for the topic since publisher was not initialized successfully")
      return false
    }
    val result = publisher.publish(schema, diff)
    result match {
      case Result.Failure => {
        logger.error(s"[${schema.topic}}] Schema Publish failed for the publisher ${publisher.getClass}")
        false
      }
      case _ => {
        logger.info(s"[${schema.topic}] Published schema using the publisher  ${publisher.getClass} for the topic at time ${Calendar.getInstance().getTime}")
        true
      }
    }
  }

  /**
   * Helper to initialize the different publishers
   * Doesn't return anything since each individual publisher stores the result of the initialization operation
   */
  def initPublishers(): Unit = {
    logger.info(s"Initializing ${publishers.length} publishers ")
    publishers.foreach(publisher => publisher.init)
  }
}
