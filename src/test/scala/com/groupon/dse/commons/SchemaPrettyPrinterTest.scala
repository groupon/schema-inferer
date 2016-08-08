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

import org.scalatest.FlatSpec

import scala.collection.immutable.HashMap

/**
 * Test for SchemaPrettyPrinter
 */
class SchemaPrettyPrinterTest extends FlatSpec {

  "The schema printer " should "return a valid tree string for a give schema " in {
    val schemaHash = HashMap("body" -> Map("method" -> "string", "eventType" -> "string", "clientPlatform" -> "string", "parameter" -> "string", "channel" -> "string", "source" -> "string", "funnelID" -> "string", "_eventfield8" -> "string",
      "_eventfield9" -> "string", "responseCode" -> "integer", "_eventfield5" -> "integer"),
      "timestamp" -> "string",
      "sourcefile" -> "string",
      "host" -> "string",
      "timestamp_ms" -> "string",
      "sourcetype" -> "string")

    val treeStr = "root\n |-- body: struct < \n |    |-- method: string \n |    |-- eventType: string \n |    |-- clientPlatform: string \n |    |-- parameter: string \n |    |-- channel: string \n |    |-- source: string \n |    |-- funnelID: string \n |    |-- _eventfield8: string \n |   " +
      " |-- _eventfield9: string \n |    |-- responseCode: integer \n |    |-- _eventfield5: integer \n |-- > \n |-- timestamp: string \n |-- sourcefile: string \n |-- host: string \n |-- timestamp_ms: string \n |-- sourcetype: string \n"

    assert(SchemaPrettyPrinter.print(schemaHash) == treeStr)
  }

  "The schema printer " should "return an empty string if the map is NULL" in {
    assert(SchemaPrettyPrinter.print(null) == "")
  }
}
