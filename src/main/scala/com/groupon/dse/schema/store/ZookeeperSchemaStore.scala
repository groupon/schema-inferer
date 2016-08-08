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

package com.groupon.dse.schema.store

import java.text.SimpleDateFormat
import java.util.Calendar

import com.groupon.dse.commons.SchemaPrettyPrinter
import com.groupon.dse.configs.AppConfigs
import com.groupon.dse.schema.{Schema, SchemaSerializationException}
import com.groupon.dse.zookeeper.ZkClientBuilder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkException
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap
import scala.util.parsing.json.JSON

/**
 * Class for Zookeeper based store
 * @param zkConnect The zk to connect to
 * @param zkRoot The zk root path
 * @param storeTime The time after which we can begin storing in zk
 */

class ZookeeperSchemaStore(zkConnect: String, zkRoot: String, storeTime: Long) extends SchemaStore  {

  @transient private var zkClient: ZkClient = null
  private val localStore = new InMemorySchemaStore
  private val logger = LoggerFactory.getLogger(getClass)
  private val canUpdateZk = new scala.collection.mutable.HashMap[String, Boolean].withDefaultValue(false)
  private val startTime = Calendar.getInstance().getTimeInMillis
  @transient implicit val formats = org.json4s.DefaultFormats
  private val sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss.SSS")
  private var connected: Boolean = true
  private val schemaRecord = new scala.collection.mutable.HashMap[Schema, Long].withDefaultValue(0)
  private val zkConnTimeout = AppConfigs.ZookeeperStateControllerConnTimeoutMs._2.toInt
  private val zkSessionTimeout = AppConfigs.ZookeeperStateControllerSessionTimeoutMs._2.toInt

/**
 * Method to get the schema from the zookeeper if local store does not have it
 * @param topic The topic of the schema
 * @throws FetchException if there is any exception caused during fetch\
 */
  def getSchemaFromZk(topic: String): Option[Schema] = {
    try {
      val schemas = ZkUtils.getChildrenParentMayNotExist(getZkClient, s"$zkRoot/$topic")

      if(schemas.isEmpty) {
        logger.debug(s"[$topic] Schema absent for the topic in the local cache and zookeeper")
        return None
      }
      //get latest schema
      val node = s"$zkRoot/$topic/current"

      //read data from zk
      val data = ZkUtils.readData(getZkClient, s"$node")._1
      val json = JSON.parseFull(data)
      //convert the json to map
      val map = json.get.asInstanceOf[HashMap[String, AnyRef]]
      logger.info(s"[$node] Found schema for the topic in the zookeeper. Not restarting the inference")

      val generic = new Schema(map, topic)
      localStore.put(generic)
      logger.info(s"[$topic] Stored schema obtained from zookeeper in local cache : ${SchemaPrettyPrinter.print(generic.content)}")

      //Set storeFlag to true, which means we can start storing in zk again
      canUpdateZk(topic) = true
      Some(generic)
    }catch {
      case zk: ZkException => {
        logger.error(s"[${topic}] Zookeeper connection exception while retrieving schema ", zk)
        throw FetchException(zk.getMessage)
      }
      case e: SchemaSerializationException => {
        logger.error(s"[${topic}] Serialization exception while retrieving schema ", e)
        throw FetchException(e.getMessage)
      }
    }
  }
  /**
   * Returns the schema from the cache. First checks the local cache if the schema exists. If not, it checks the zookeeper if any schemas exist for the topic.
   * Returns null if schema is not present in either.
   * @throws FetchException if there is any exception caused during fetch\
   */
  override def get(topic: String): Option[Schema] = {
    val schema = localStore.get(topic)
    schema match {
      case None => getSchemaFromZk(topic)
      case _ => {
        updateSchemaStore(schema.orNull)
        schema
      }
    }
  }

  /**
   * Total number of schemas stored locally
   * @return
   */
  override def size: Int = localStore.size

  /**
   * Stores the schema in the local cache and the zookeeper if the 'canUpdateZk' flag is set. Otherwise only stores it locally.
   * The 'canUpdateZk' flag is used to ensure that during initial period when there would be many changes, we dont update the zk until a certain time period expires.
   * @param schema The schema to strore
   * @return true if success , false otherwise
   */
  override def put(schema: Schema): Boolean = {
    localStore.put(schema)
    if(canUpdateZk(schema.topic)){
      schemaRecord(schema) = Calendar.getInstance().getTimeInMillis
    }
    true
  }

  /**
   * Check if the cache contains the schema for the topic
   * @param topic The schema topic
   * @return true if exists, false otherwise
   */
  override def hasKey(topic: String): Boolean = localStore.hasKey(topic) || zkContains(topic)

  /**
   * Flush all/dirty locally stored schemas to the remote store. Only applies to remote schema stores like zookeeper
   */
  override def flush: Unit = {
    localStore.getTopics.foreach { topic =>
      val schemasForTopic = schemaRecord.filterKeys(schema => schema.topic == topic)
      if(schemasForTopic.size > 0) {
        val maxTimestamp = schemasForTopic.valuesIterator.max
        logger.debug(s"[$topic] Flushing the topic schemas to the zookeeper. Number of schemas for the topic : ${schemasForTopic.size} ")
        schemasForTopic.foreach({
          schemaTimestamp => updateZookeeper(schemaTimestamp._1, schemaTimestamp._2, maxTimestamp)
        })
      }
    }
  }

  /**
   * Serializes the map and stores it in the zookeeper in the form :/root/$topic/$topic-timestamp
   * @param schema the schema to store
   * @param timestamp The timestamp the schema was generated
   * @param maxTimestamp The maxTimestamp of the list of schemas for a given topic
   * @return true id success, false if any exceptions
   */
  def updateZookeeper(schema: Schema, timestamp: Long, maxTimestamp: Long): Boolean = {
    try{
      val str = schema.toJsonString
      var creationTime = getTime(timestamp)
      val node = s"$zkRoot/${schema.topic}/${schema.topic}_$creationTime"
      val currentNode = s"$zkRoot/${schema.topic}/current"

      //Creates a node with the current timestamp. Also updates the current node to be this one.
      //This way we dont need to do any renames when "current" node changes
      ZkUtils.updatePersistentPath(getZkClient, node ,str)
      if(timestamp == maxTimestamp) {
        ZkUtils.updatePersistentPath(getZkClient, currentNode, str)
        logger.debug(s"[${schema.topic}] Updated the current node in the zookeeper with the schema :$node")
      }

      logger.debug(s"[${schema.topic}] Updated the zookeeper with new schema :$node")
      schemaRecord.remove(schema)
      true
    }catch{
      case e: ZkException => {
        logger.error(s"[${schema.topic}]Zookeeper connection exception while storing schema. Setting topic to dirty state since only local store updated", e)
        close
        false
      }
    }
  }

  /**
   * Check if the zk contains the topic
   * @param topic the schema topic
   * @return true if it contains, false otherwise
   */
  private def zkContains(topic: String) : Boolean =  ZkUtils.pathExists(getZkClient, s"$zkRoot/$topic")

  /**
   * Update the (schema topic -> boolean) flag to indicate if the configured time has expired. This means we can start updating the zk for any schema change now
   * @param schema The schema
   */
  private def updateSchemaStore(schema: Schema): Unit = {
    if(!canUpdateZk(schema.topic)) {
      if (Calendar.getInstance().getTimeInMillis - startTime > storeTime * 1000) {
        logger.info(s"[${schema.topic}] Can start writing to zookeeper now. ZK store time exceeded")
        canUpdateZk(schema.topic) = true
        if (localStore.hasKey(schema.topic)) {
          logger.info(s"[${schema.topic}] Local cache contains schema for the topic, Setting flag to flush the topic")
          schemaRecord(schema) = Calendar.getInstance().getTimeInMillis
        }
      }
    }
  }

  /**
   * Converts millisecond timestamp to yyyy-MM-dd-HH:mm:ss.SSS
   * @param timeInMillis millisecond timestamp
   * @return yyyy-MM-dd-HH:mm:ss.SSS representation of the timestamp
   */
  private def getTime(timeInMillis: Long): String = sdf.format(timeInMillis)

  /**
   * Check if the zkClient valid else create a new one
   *
   * @return ZkClient instance
   */
  private def getZkClient: ZkClient = {
    if (zkClient == null || !connected) {
      logger.debug("Creating new zkClient.")
      zkClient = ZkClientBuilder(zkConnect, zkConnTimeout, zkSessionTimeout)
      connected = true
    }
    zkClient
  }

  /**
   * Close any resources opened for the controller
   */
  def close = {
    try {
      connected = false
      zkClient.close()
    } catch {
      case e: Exception => logger.error("Failed to close the ZookeeperStateController zkClient", e)
    }
  }


}
