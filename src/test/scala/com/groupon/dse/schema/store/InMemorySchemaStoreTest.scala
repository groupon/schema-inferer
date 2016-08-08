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

import com.groupon.dse.schema.Schema
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.collection.immutable.HashMap

/**
 * Test for In memory schema store
 */
class InMemorySchemaStoreTest extends FlatSpec with BeforeAndAfter {
  var store: InMemorySchemaStore = _
  before {
    store = new InMemorySchemaStore
  }

  "The store " should "return null if there is no schema for the sourceType" in {
    assert(store.get("invalid_topic") == None)
  }

  "The store " should "return the appropriate schema for the sourceType after it is stored" in {
    val schemaHash_saved: HashMap[String, AnyRef]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "_eventfield8" -> "string"))
    val savedSchema = new Schema(schemaHash_saved, "topic1")
    store.put(savedSchema)
    assert(store.get("topic1") == Some(savedSchema))
  }

  it should "return true if the store contains the schema and false otherwise " in {
    val savedSchema = new Schema(null, "topic1")
    store.put(savedSchema)
    assert(store.hasKey("topic1"))
    assert(!store.hasKey("invalid_topic"))
  }

  it should "return the size equal to the total number of schemas stored in the store " in {
    assert(store.size == 0)
    val savedSchema = new Schema(null, "topic")
    val savedSchema1 = new Schema(null, "topic1")

    store.put(savedSchema)
    store.put(savedSchema1)
    assert(store.size == 2)
  }

  it should "return the total number of topics stored in the store" in {
    val savedSchema = new Schema(null, "topic")
    val savedSchema1 = new Schema(null, "topic1")

    store.put(savedSchema)
    store.put(savedSchema1)

    assert(store.getTopics.length == 2)
  }

}
