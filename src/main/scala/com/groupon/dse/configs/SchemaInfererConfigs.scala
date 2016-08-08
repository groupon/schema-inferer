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

import com.groupon.dse.commons.{StartPolicy, StartPolicyBuilder}
import com.groupon.dse.schema.store.{SchemaStore, SchemaStoreBuilder}
import org.apache.spark.SparkContext

/**
 * Case class to store the schema inferer related configs
 * @param startPolicy The publisher start policy
 * @param store The schema store object
 */
case class SchemaInfererConfigs(
                          startPolicy: StartPolicy,
                          store: SchemaStore,
                          topics: Traversable[String],
                          samplingRatio: String
                          )

object SchemaConfigBuilder {

  def apply(properties: Properties, sc: SparkContext) : SchemaInfererConfigs = {
    val startPolicy = StartPolicyBuilder(properties)
    val store = SchemaStoreBuilder(properties)
    val topics = properties.getProperty(PluginConfigs.Topics._1).split(",").map(_.trim)
    val samplingRatio = properties.getProperty(PluginConfigs.SamplingRatio._1, PluginConfigs.SamplingRatio._2)

    SchemaInfererConfigs(startPolicy, store, topics, samplingRatio)
  }
}