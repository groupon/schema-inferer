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

package com.groupon.dse.schema.publisher

import com.groupon.dse.schema.Schema
import com.groupon.dse.schema.publisher.Result.Result
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap

/**
 * Publisher that logs to a file using the log4j configuration
 */
class Log4jSchemaPublisher extends SchemaPublisher{
  private val logger = LoggerFactory.getLogger(getClass)
  /**
   * Initialize the publisher
   * @return The result of the initialization ie SUCCESS or FAILURE
   */
  override def init : Unit = logger.info("Initializing")

  /**
   * Return if the publisher initialization was successful. Always called for each publisher before publishing any changes.
   * @return the result of the initialization ie true or false
   */
  override def isInitialized: Boolean = true

  /**
   * Publish the schema
   * @param schema The schema to publish
   * @return The result of the operation ie SUCCESS or FAILURE
   */
  override def publish(schema: Schema, diff: HashMap[String, Any]): Result = {
    if(schema != null){
      logger.info(s"[${schema.topic}] Publishing schema : ${schema.toJsonString} ")
    }
    Result.Success
  }
}