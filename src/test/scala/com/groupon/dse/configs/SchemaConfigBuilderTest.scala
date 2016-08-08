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

package com.groupon.dse.configs

import java.util.Properties

import com.groupon.dse.commons.StartPolicy
import com.groupon.dse.schema.store.SchemaStore
import org.scalatest.FlatSpec

/**
 * Test for schema config builder
 */
class SchemaConfigBuilderTest extends FlatSpec {

  "The schema config builder" should "return a schema config object having a publisher, store and the start policy" in {
    val config = SchemaConfigBuilder(getSchemaInfererProperties, null)
    assert(config.startPolicy.isInstanceOf[StartPolicy])
    assert(config.store.isInstanceOf[SchemaStore])
    assert(config.topics.isInstanceOf[Traversable[String]])
    assert(config.topics.size == 2)
    assert(config.samplingRatio.toDouble == 0.3)
  }

  def getSchemaInfererProperties: Properties = {
    val properties = new Properties()
    properties.setProperty("topic.warmup.policy", "TIME")
    properties.setProperty("topic.warmup.time.sec", "20")
    properties.setProperty("topics", "test_topic_1, test_topic_2")

    properties.setProperty("publisher.type","DEFAULT")

    properties.setProperty("store.type","ZOOKEEPER")
    properties.setProperty("store.zk.connect","localhost:2181")
    properties.setProperty("store.zk.root","/test_store")
    properties.setProperty("rdd.sampling.ratio","0.3")
    properties
  }
}
