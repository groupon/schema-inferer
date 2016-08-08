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

import com.groupon.dse.commons.Utils._
import org.scalatest.FlatSpec

import scala.collection.immutable.HashMap

/**
 * Test for utils
 */
class UtilsTest extends FlatSpec {

  "2 maps having new keys " should "merge to create a new map having the union of the 2 maps" in {
    val map1 = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "parameter" -> "string", "channel" -> "string", "source" -> "string", "funnelID" -> "string", "_eventfield8" -> "string",
      "_eventfield9" -> "string", "responseCode" -> "integer", "_eventfield5" -> "integer"),
      "timestamp" -> "string",
      "sourcefile" -> "string",
      "host" -> "string",
      "timestamp_ms" -> "string",
      "sourcetype" -> "string")

    val map2: HashMap[String, Any]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "parameter" -> "string", "channel" -> "string", "source" -> "string", "funnelID" -> "integer", "_eventfield8" -> "string", "new_field" -> "int"))

    val result: HashMap[String, Any]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "parameter" -> "string", "channel" -> "string", "source" -> "string", "funnelID" -> "string", "_eventfield8" -> "string",
      "_eventfield9" -> "string", "responseCode" -> "integer", "_eventfield5" -> "integer", "new_field" -> "int"),
      "timestamp" -> "string",
      "sourcefile" -> "string",
      "host" -> "string",
      "timestamp_ms" -> "string",
      "sourcetype" -> "string")

    assert((map1 deepMerge map2).asInstanceOf[HashMap[String, Any]] == result)
  }

  "Comparing 2 schema with a change in key types " should "return a hashmap containing the diff if the new key has higher precedence" in {

    val map1 = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "parameter" -> "string", "channel" -> "string", "source" -> "string", "funnelID" -> "string", "_eventfield8" -> "string",
      "_eventfield9" -> "string", "responseCode" -> "integer", "_eventfield5" -> "integer"),
      "timestamp" -> "string",
      "sourcefile" -> "string",
      "host" -> "string",
      "timestamp_ms" -> "string",
      "sourcetype" -> "string")


    val map2: HashMap[String, Any]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "parameter" -> "string", "channel" -> "string", "source" -> "string", "funnelID" -> "integer", "_eventfield8" -> "string",
      "_eventfield9" -> "string", "responseCode" -> "integer", "_eventfield5" -> "integer"),
      "timestamp" -> "string",
      "sourcefile" -> "string",
      "host" -> "string",
      "timestamp_ms" -> "string",
      "sourcetype" -> "string")

    assert((map2 compareMaps  map1) == HashMap("body" -> Map("funnelID" -> "string  -- integer")))
  }

  "Comparing 2 schemas having no difference " should "return null" in {
    val map1: HashMap[String, Any]  = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "parameter" -> "string", "channel" -> "string", "source" -> "string", "funnelID" -> "integer", "_eventfield8" -> "string", "new_field" -> "int"))
    assert((map1 compareMaps map1) == null)
  }

  "Comparing 2 schemas having the lists different " should "return the list having a type of higher precedence" in {
    val map1: HashMap[String, Any]  = HashMap("body" -> List("string"))
    val map2: HashMap[String, Any]  = HashMap("body" -> List(Map("key1" -> "int")))

    val result = HashMap("body" -> List("string -- Map(key1 -> int)"))

    assert((map1 compareMaps map2) == result)
  }

  "Comparing 2 arrays having different element types " should " return null if the new array has element type of less precedence " in {
    val array1 = List("int")
    val array2 = List("string")

    assert((array2 compareLists(array1)) == null)
    val diff = array1 compareLists(array2)
    val result = List("int -- string")

    assert(diff == result)
  }

  "Comparing 2 arrays having elements which are maps or arrays " should " return the appropriate difference" in {
    val array1 = List(Map("method" -> "string", "Platform" -> "string", "channel" -> "string"))
    val array2 = List(Map("method" -> "string", "Platform" -> "string", "channel" -> "string",  "_eventfield8" -> "string", "new_field" -> "string"))

    val diff1 = array1 compareLists(array2)
    val result1 = List(Map("_eventfield8" -> "string", "new_field" -> "string"))

    assert(diff1 == result1)

    val array3 = List(List("int"))
    val array4 = List(List("string"))

    val result2 = List(List("int -- string"))
    val diff2 = array3 compareLists(array4)

    assert(diff2 == result2)
  }

  "Comparing 2 arrays/lists " should "return the appropriate difference" in {

    val list1 = List(Map("method" -> "string", "Platform" -> "string", "channel" -> "string"))
    val list2 = List(Map("method" -> "string", "Platform" -> "string", "channel" -> "string",  "_eventfield8" -> "string"))

    assert(Utils.compareValues(list2, list1) == list2)
  }

  "Merging 2 arrays/lists " should "return the appropriate merged list" in {

    val list1 = List(Map("method" -> "string", "Platform" -> "string", "channel" -> "string", "newfield" -> "boolean"))
    val list2 = List(Map("method" -> "string", "Platform" -> "string", "channel" -> "string",  "_eventfield8" -> "string"))
    val list3 = List(Map("method" -> "string", "Platform" -> "string", "channel" -> "string",  "_eventfield8" -> "string", "newfield" -> "boolean"))
    assert((list1 mergeList list2) == list3)

    assert((Utils.mergeValues(list1, list2)) == list3)
  }
}
