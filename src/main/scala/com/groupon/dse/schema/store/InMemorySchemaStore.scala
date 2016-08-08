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
import org.slf4j.LoggerFactory

/**
 * In memory hash table based schema cache
 */
class InMemorySchemaStore extends SchemaStore {
  val logger = LoggerFactory.getLogger(getClass)
  private val hashStore = scala.collection.mutable.HashMap.empty[String, Schema]

  /**
   * method to fetch the schema from the in memory store
   * @param topic topic of the schema to return
   * @return the schema corresponding to the sourcetype
   */
  override def get(topic: String): Option[Schema] = hashStore.get(topic)

  /**
   * Stores the schema in the cache
   * @param schema the schema to store
   */
  override def put(schema: Schema): Boolean = {
    val topic = schema.topic
    hashStore += topic -> schema
    true
  }

  /**
   * Test if the cache contains the topic schema
   * @param topic the topic whose schema we need to check if present in the cache
   * @return true if present, false otherwise
   */
  override def hasKey(topic: String): Boolean = hashStore.contains(topic)

  /**
   * @return size of the cache
   */
  override def size: Int = hashStore.size

  /**
   * Flush all the locally stored schemas to the remote store. Only applies to remote schema stores like zookeeper
   */
  override def flush: Unit = logger.debug("In memory store does not need this implementation")

  def getTopics: List[String] = hashStore.keySet.toList

}
