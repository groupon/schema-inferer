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

import java.util.Properties

import com.groupon.dse.configs.PluginConfigs.MissingConfigException
import com.groupon.dse.schema.store.SchemaStoreBuilder.InvalidStoreTypeException
import org.scalatest.FlatSpec

/**
 * Test for schema store builder
 */
class SchemaStoreBuilderTest extends FlatSpec {

  it should "return the instance of the cache based on the type set in the config" in {
    val cacheProperties = new Properties()
    cacheProperties.setProperty("store.type","HASH")
    assert(SchemaStoreBuilder(cacheProperties).isInstanceOf[InMemorySchemaStore])
    assert(!SchemaStoreBuilder(cacheProperties).isInstanceOf[ZookeeperSchemaStore])

    val cacheProperties1 = new Properties()
    cacheProperties1.setProperty("store.type","ZOOKEEPER")
    cacheProperties1.setProperty("store.zk.connect","localhost:2181")
    cacheProperties1.setProperty("store.zk.root","/")
    assert(SchemaStoreBuilder(cacheProperties1).isInstanceOf[ZookeeperSchemaStore])
  }

  it should "throw an exception if there are any missing configs" in {
    val cacheProperties = new Properties()
    cacheProperties.setProperty("store.type","ZOOKEEPER")
    cacheProperties.setProperty("store.zk.connect","localhost:2181")

    intercept[MissingConfigException] {
      SchemaStoreBuilder(cacheProperties)
    }
  }

  it should "throw an exception if the cache type is invalid" in {
    val cacheProperties = new Properties()
    cacheProperties.setProperty("store.type","INVALID")
    intercept[InvalidStoreTypeException] {
      SchemaStoreBuilder(cacheProperties)
    }
  }
}
