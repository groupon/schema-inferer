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

package com.groupon.dse.schema

import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.collection.immutable.HashMap

/**
 * Test for schema class
 */
class SchemaTest extends FlatSpec with BeforeAndAfter {

  var schema: Schema = _
  var schemaHash: HashMap[String, AnyRef] = _


  before{

    schemaHash = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "parameter" -> "string", "channel" -> "string", "source" -> "string", "funnelID" -> "string", "_eventfield8" -> "string",
                                       "_eventfield9" -> "string", "responseCode" -> "integer", "_eventfield5" -> "integer"),
                         "timestamp" -> "string",
                         "sourcefile" -> "string",
                         "host" -> "string",
                         "timestamp_ms" -> "string",
                         "sourcetype" -> "string")


    schema = new Schema(schemaHash, "topic1")
  }

  "The schema map " must "be equal to the map set during instantiation" in {
    assert(schema.content == schemaHash)
  }

  "The schema topic " must "be equal to the topic in the configs set during instantiation" in {
    assert(schema.topic == "topic1")
  }

  "Comparing the current schema and null schema at startup " must "return the current schema as the difference" in {
    assert(schema.diff(null) == schemaHash)
  }

  "Comparing 2 schemas " must "return the keys not present in the compared schema " in {

    val schemaHash_saved: HashMap[String, AnyRef]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "parameter" -> "string", "channel" -> "string", "source" -> "string", "funnelID" -> "string", "_eventfield8" -> "string"),
                                                          "timestamp" -> "string",
                                                          "sourcefile" -> "string",
                                                          "host" -> "string",
                                                          "timestamp_ms" -> "string",
                                                          "sourcetype" -> "string")
    val savedSchema = new Schema(schemaHash_saved, "topic1")
    assert(schema.diff(savedSchema) == HashMap("body" -> Map("_eventfield9" -> "string", "responseCode" -> "integer", "_eventfield5" -> "integer")))
  }

  "Comparing 2 schemas " must "return the keys whose type is of higher precedence in this schema " in {

    val schemaHash_saved: HashMap[String, AnyRef]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "parameter" -> "string", "channel" -> "string", "source" -> "string", "funnelID" -> "integer", "_eventfield8" -> "string",
                                                          "_eventfield9" -> "string", "responseCode" -> "integer", "_eventfield5" -> "integer"),
                                                          "timestamp" -> "string",
                                                          "sourcefile" -> "string",
                                                          "host" -> "string",
                                                          "timestamp_ms" -> "string",
                                                          "sourcetype" -> "string")
    val savedSchema = new Schema(schemaHash_saved, "topic1")
    assert(schema.diff(savedSchema) == HashMap("body" -> Map("funnelID" -> "string  -- integer")))
  }

  "Comparing 2 schemas having no difference " must "return null" in {
    assert(schema.diff(schema) == null)
  }

  "Merging 2 schemas having a difference " must "return a new schema having the union of keys in both schemas " in {
    val schemaHash_saved: HashMap[String, AnyRef]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "parameter" -> "string", "channel" -> "string", "source" -> "string", "funnelID" -> "integer", "_eventfield8" -> "string", "new_field" -> "int"))
    val savedSchema = new Schema(schemaHash_saved, "topic1")

    val schemaHash_merged: HashMap[String, Any]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "parameter" -> "string", "channel" -> "string", "source" -> "string", "funnelID" -> "string", "_eventfield8" -> "string",
                                                           "_eventfield9" -> "string", "responseCode" -> "integer", "_eventfield5" -> "integer", "new_field" -> "int"),
                                                           "timestamp" -> "string",
                                                           "sourcefile" -> "string",
                                                           "host" -> "string",
                                                           "timestamp_ms" -> "string",
                                                           "sourcetype" -> "string")
    assert(schema.union(savedSchema).content == schemaHash_merged)
  }

  "Merging this schema with a null schema "must "return schema having the same content" in {
    assert(schema.union(null).content == schema.content)
  }

  it should "return the appropriate tree string if schema string is called" in {
    val treeStr = "root\n |-- body: struct < \n |    |-- method: string \n |    |-- eventType: string \n |    |-- clientPlatform: string \n |    |-- parameter: string \n |    |-- channel: string \n |    |-- source: string \n |    |-- funnelID: string \n |    |-- _eventfield8: string \n |   " +
                  " |-- _eventfield9: string \n |    |-- responseCode: integer \n |    |-- _eventfield5: integer \n |-- > \n |-- timestamp: string \n |-- sourcefile: string \n |-- host: string \n |-- timestamp_ms: string \n |-- sourcetype: string \n"

    assert(schema.prettyPrint == treeStr)
  }

  "The diff returned by the getter " should "be equal to the diff set" in {
    val diff = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string"))
    val schema = new Schema(schemaHash, "topic1", diff)
    assert(schema.parentDiff == diff)
  }

  "The json string representation of the schema " should "be accurate" in {
    val json = "{\"body\":{\"method\":\"string\",\"eventType\":\"string\",\"clientPlatform\":\"string\",\"parameter\":\"string\",\"channel\":\"string\",\"source\":\"string\",\"funnelID\":\"string\",\"_eventfield8\":\"string\",\"_eventfield9\":\"string\",\"responseCode\":\"integer\",\"_eventfield5\":\"integer\"},\"timestamp\":\"string\",\"sourcefile\":\"string\",\"host\":\"string\",\"timestamp_ms\":\"string\",\"sourcetype\":\"string\"}"
    assert(schema.toJsonString == json)
  }

  it should "return a null json string if the map is null" in {
    val schema1 = new Schema(null, "topic1")
    assert(schema1.toJsonString == "null")
  }
}
