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

package com.groupon.dse.commons

import org.apache.spark.sql.types._
import org.scalatest.FlatSpec

/**
 * Test for sparkSchemaNormalizer
 */
class SchemaNormalizerTest extends FlatSpec {

  it should "convert a spark schema of a raw data stream to an appropriate map" in {
    val structtype = StructType(List(StructField("field1", IntegerType), StructField("field2", StringType), StructField("field3", LongType), StructField("field4", StringType)))
    val result = Map("field1" -> "integer", "field4" -> "string", "field3" -> "long", "field2" -> "string")
    assert(SchemaNormalizer.normalize(structtype, "topic1").content == result)
  }

  it should "convert a complex spark schema to a Schema object" in {
    val structtype = StructType(List(StructField("field1", IntegerType), StructField("field2", StructType(List(StructField("mapField1" , StringType), StructField("mapField2" , IntegerType), StructField("mapField3" , ArrayType(IntegerType))))), StructField("field3", LongType), StructField("field4", StringType)))
    val result = Map("field1" -> "integer", "field4" -> "string", "field3" -> "long", "field2" -> Map("mapField2" -> "integer", "mapField1" -> "string", "mapField3" -> List("integer")))
    assert(SchemaNormalizer.normalize(structtype, "topic1").content == result)
  }
}
