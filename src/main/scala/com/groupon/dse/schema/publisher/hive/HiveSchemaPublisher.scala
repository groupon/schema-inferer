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

package com.groupon.dse.schema.publisher.hive

import com.groupon.dse.schema.Schema
import com.groupon.dse.schema.publisher.Result.Result
import com.groupon.dse.schema.publisher.{Result, SchemaPublisher}
import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap

/**
 * The Hive publisher class
 * @param db The hive database to create a table in
 * @param serde The serde jar location
 * @param serdeClass The serde class
 */
class HiveSchemaPublisher(db: String, serde: String, serdeClass: String, blacklist: Array[String], sc: SparkContext) extends SchemaPublisher {

  @transient private lazy val hc:HiveContext = new HiveContext(sc)
  private val logger = LoggerFactory.getLogger(getClass)
  private var initialized = false

  /**
   * Initialization method to set certain configs like which db, serde to use
   * @return True if init successful, false otherwise
   */
  override def init: Unit = {
    try {
      hc.sql(s"use $db")
      hc.sql(s"add jar $serde")
      initialized = true
    }catch{
      case e@(_: QueryExecutionException) =>
        logger.error("Exception while executing spark query during publisher init stage, exiting.",e.getStackTraceString)
        initialized = false
      case e@(_: RuntimeException) =>
        logger.error("Exception during publisher init stage, exiting. Hive metastore might be unreachable",e.getStackTraceString)
        initialized = false
    }
  }

  /**
   * Return if the publisher initialization was successful. Always called for each publisher before publishing any changes.
   * @return the result of the initialization ie true or false
   */
  override def isInitialized: Boolean = initialized

  /**
   * Publishes the schema to hive. Handles errors during creation and dropping.
   * @param mergedSchema The generic schema to publisj
   * @return The result (success/fail/revert)
   */
  override def publish(mergedSchema: Schema, diff: HashMap[String, Any]): Result = {
   if(blacklist.contains(mergedSchema.topic)){
      logger.info(s"[${mergedSchema.topic}] Skipping publish schema for blacklisted topic ")
      return Result.Success
    }
    val table = HiveQueryBuilder.createTableQueryForSchema(mergedSchema, serdeClass)

    try{
      updateHive(table, mergedSchema.topic)
    }catch {
      case e@(_: HiveException) =>
        logger.error(s"[${mergedSchema.topic}] Error updating table in hive metastore : ${e.getStackTraceString}")
        return Result.Failure
    }
    Result.Success
  }

  /**
   * Makes the actual calls to connect to the hive metastore and create/drop tables
   * @param query The hive schema string
   * @param topic The topic of the schema string
   * @throws HiveException Exception thrown if there is an error while creating/dropping table
   */
  @throws(classOf[HiveException])
  private def updateHive(query: String, topic: String): Unit = {
    try {
      logger.info(s"[$topic] Dropping hive table")
      hc.sql(s"drop table $topic")
    }catch{
      case e@(_: Exception)  =>
        throwHivePublisherException(s"Error dropping hive table for $topic", e)
    }
    try{
      logger.info(s"[$topic] Creating new hive table ${query}")
      hc.sql(query)
    } catch{
      case e@(_: Exception)  =>
        throwHivePublisherException(s"Error creating hive table for $topic", e)
    }
  }

  private def throwHivePublisherException(msg: String, e: Exception) = {
    logger.error(msg,e)
    throw HiveException(e.getMessage)
  }

  /**
   * Case class to handle exceptions during creating tables
   * @param message error message
   */
  case class HiveException(message: String) extends Exception(message)
}
