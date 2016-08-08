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

/**
 * Trait to cache the schema for comparison and record keeping
 */
trait SchemaStore extends Serializable {

  /**
   * Get the schema from the schema store
   * @param topic The topic whose schema we want to get
   * @return The schema for the topic
   */
  def get(topic: String): Option[Schema]

  /**
   * Check if the store contains the schema for the given topic
   * @param topic The topic to check if it exists in the store
   * @return True if it exists, false otherwise
   */
  def hasKey(topic: String): Boolean

  /**
   * Size of the schema store
   * @return the number of topic -> schemas stored
   */
  def size: Int

  /**
   * Stores the given schema in the store
   * @param schema the schema to store
   * @return true if store successful, false otherwise
   */
  def put(schema: Schema): Boolean

  /**
   * Flush all the locally stored schemas to the remote store. Only applies to remote schema stores like zookeeper
   */
  def flush: Unit
}

/**
 * Case class for handling fetch exceptions from the schema store
 * @param message The error message
 */
case class FetchException(message: String) extends Exception(message)