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
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.collection.immutable.HashMap

/**
 * Tests for hive query builder
 */
class HiveQueryBuilderTest extends FlatSpec with BeforeAndAfter {

  "The  HiveQueryBuilder "should "generate a syntactically correct hive query give a schema  " in {
    val content: HashMap[String, AnyRef]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "_eventfield8" -> "string", "list_key" -> List(Map("method" -> "string", "Platform" -> "string", "channel" -> "string"))))
    val schema = new Schema(content, "test")
    val expected_query = "CREATE EXTERNAL TABLE test ( body struct< method:string, eventType:string, clientPlatform:string, `_eventfield8`:string, list_key:array<  struct< method:string, Platform:string, channel:string>>>)PARTITIONED BY (date string, hour string) ROW FORMAT SERDE 'SERDE_CLASS' STORED AS sequencefile LOCATION '/user/kafka/source=test'"
    assert(HiveQueryBuilder.createTableQueryForSchema(schema, "SERDE_CLASS") == expected_query)
  }

  it should "transform the given schema to a schema compatible with hive" in {
    val content: HashMap[String, AnyRef]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "_eventfield8" -> "string", "list_key" -> List(Map("method" -> "string", "Platform" -> "string", "channel" -> "string"))))
    val schema = new Schema(content, "test")
    val expected_schema = " body struct< method:string, eventType:string, clientPlatform:string, `_eventfield8`:string, list_key:array<  struct< method:string, Platform:string, channel:string>>>"
    assert(HiveQueryBuilder.transformSchema(schema) == expected_schema)
  }

  it should "process a nested map and convert it to a nested hive struct" in {
    val content: HashMap[String, AnyRef]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "_eventfield8" -> "string"))
    val strBuilder = new StringBuilder
    HiveQueryBuilder.processMap(strBuilder, content)
    val expected_result = "< body:struct< method:string, eventType:string, clientPlatform:string, `_eventfield8`:string>>,"
    assert(strBuilder.toString() == expected_result)
  }

  it should "process a list and convert it into a hive array" in {
    val content: HashMap[String, AnyRef] = HashMap("body" -> List("string"))
    val strBuilder = new StringBuilder
    HiveQueryBuilder.processMap(strBuilder, content)
    val expected_result = "< body:array<  string>>,"
    assert(strBuilder.toString() == expected_result)
  }

  it should "convert a give datatype to a datatype compatible with hive" in {
    val type1 = "long"
    val type2 = "integer"
    val type3 = "struct"
    assert(HiveQueryBuilder.convertDataType(type1) == "bigint")
    assert(HiveQueryBuilder.convertDataType(type2) == "int")
    assert(HiveQueryBuilder.convertDataType(type3) == "struct")
  }

  it should "convert a given datatype into one compatible with hive" in {
    val content: HashMap[String, AnyRef] = HashMap("body" -> List("string"))
    val strBuilder = new StringBuilder
    HiveQueryBuilder.processField(strBuilder, "field1", Option("int"))
    assert(strBuilder.toString() == " field1:int,")
    strBuilder.clear()
    HiveQueryBuilder.processField(strBuilder, "field2" ,Some(content))
    assert(strBuilder.toString().replaceAll("\\s", "") == "field2:struct<body:array<string>>,")
  }

  it should "convert a field name into one compatible with hive "in{
    assert(HiveQueryBuilder.processFieldName("valid") == "valid")
    assert(HiveQueryBuilder.processFieldName("in-valid") == "in_valid")
    assert(HiveQueryBuilder.processFieldName("_invalid") == "`_invalid`")
    assert(HiveQueryBuilder.processFieldName("in.valid") == "`in.valid`")
  }

  it should "return the correct string representation of the give datatype" in{
    assert(HiveQueryBuilder.getDataType(Option("int")) == "int")
    assert(HiveQueryBuilder.getDataType(Option(HashMap("body" -> List("string")))) == "struct")
    assert(HiveQueryBuilder.getDataType(Option(List("string"))) == "array")
  }
}
