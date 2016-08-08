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

import com.groupon.dse.commons.{EmbeddedZookeeper, TestUtils}
import com.groupon.dse.configs.AppConfigs
import com.groupon.dse.schema.Schema
import com.groupon.dse.zookeeper.ZkClientBuilder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.collection.immutable.HashMap


/**
 * Test for zk schema store
 */
class ZookeeperSchemaStoreTest extends FlatSpec with BeforeAndAfter {

  var zk: EmbeddedZookeeper = _
  var store: ZookeeperSchemaStore = _
  val sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss.SSS")

  before {
    zk = new EmbeddedZookeeper(TestUtils.choosePorts(1)(0))
    store = new ZookeeperSchemaStore(zk.connectString, "/test", 10)
  }

  after {
    zk.shutdown()
  }

  "The store " should "return true if we try to save a schema before the time to store in zk is exceeded" in {
    val schemaHash_saved: HashMap[String, AnyRef]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "_eventfield8" -> "string"))
    val savedSchema = new Schema(schemaHash_saved, "topic1")
    assert(store.put(savedSchema))
  }

  "The store" should "return null if there is no schema stored in the local store as well as zookeeper" in {
    assert(store.get("Invalid").orNull == null)
  }

  "The store" should "return the schema if it is present in the local store and not in the zk" in {

    val schemaHash_saved: HashMap[String, AnyRef]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "_eventfield8" -> "string"))
    val savedSchema = new Schema(schemaHash_saved, "topic1")
    store.put(savedSchema)
    assert(store.get("topic1") == Some(savedSchema))

  }

  "The store" should " store the schema in the zk if the store time has been crossed" in {

    val localCache = new ZookeeperSchemaStore(zk.connectString, "/test", -1)
    val zkClient = ZkClientBuilder(zk.connectString,AppConfigs.ZookeeperStateControllerConnTimeoutMs._2.toInt,AppConfigs.ZookeeperStateControllerSessionTimeoutMs._2.toInt)
    val schemaHash_saved: HashMap[String, AnyRef]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "_eventfield8" -> "string"))
    val savedSchema = new Schema(schemaHash_saved, "topic1")
    localCache.put(savedSchema)
    localCache.get("topic1")
    localCache.flush
    assert(ZkUtils.getChildren(zkClient, "/test/topic1").length == 2) //2 since we store current as well as timestamp node

  }

  "The remote store" should " not store the schema in the zk if the store time has not been crossed" in {

    val zkClient = ZkClientBuilder(zk.connectString,AppConfigs.ZookeeperStateControllerConnTimeoutMs._2.toInt,AppConfigs.ZookeeperStateControllerSessionTimeoutMs._2.toInt)
    val schemaHash_saved: HashMap[String, AnyRef] = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "_eventfield8" -> "string"))
    val savedSchema = new Schema(schemaHash_saved, "topic1")

    store.put(savedSchema)
    store.flush

    intercept[ZkNoNodeException] {
      ZkUtils.getChildren(zkClient, "/test/topic1")
    }
  }

  it should "throw FetchException if there is a zk connection error while trying to fetch " in {
    val localZk = new ZookeeperSchemaStore("invalid:2181", "/test", 10)
    intercept[FetchException]{
      localZk.get("topic1")
    }
  }

  "The store" should "return the size to be equal to the schemas stored" in{
    assert(store.size == 0)

    val schemaHash_saved: HashMap[String, AnyRef] = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "_eventfield8" -> "string"))
    var savedSchema = new Schema(schemaHash_saved, "topic1")
    store.put(savedSchema)
    savedSchema = new Schema(schemaHash_saved, "topic2")
    store.put(savedSchema)
    assert(store.size == 2)
  }

  "The store " should "return false if the schema is not present locally or in the zookeeper and true otherwise" in {
    val zkClient = ZkClientBuilder(zk.connectString,AppConfigs.ZookeeperStateControllerConnTimeoutMs._2.toInt,AppConfigs.ZookeeperStateControllerSessionTimeoutMs._2.toInt)
    val node = "/test/topic1/topic1_schema"
    ZkUtils.updatePersistentPath(zkClient, node ,"test data")
    assert(store.hasKey("topic1"))
    assert(!store.hasKey("invalid"))
  }

  "The store" should "update the zookeeper and return true if successful" in {
    val schemaHash_saved: HashMap[String, AnyRef] = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "_eventfield8" -> "string"))
    val savedSchema = new Schema(schemaHash_saved, "topic1")
    assert(store.updateZookeeper(savedSchema,1,1))
  }

  it should "return false if the zookeeper is unreachable/invalid and we try to update" in {
    val localZk = new ZookeeperSchemaStore("invalid", "/test", -1)
    val schemaHash_saved: HashMap[String, AnyRef] = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "_eventfield8" -> "string"))
    val savedSchema = new Schema(schemaHash_saved, "topic1")
    assert(!localZk.updateZookeeper(savedSchema,1,1))
  }
}


