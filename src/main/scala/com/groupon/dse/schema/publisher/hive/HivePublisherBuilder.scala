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

import java.util.Properties

import com.groupon.dse.configs.PluginConfigs
import org.apache.spark.SparkContext


/**
* Object to create a publisher which the user defines
*/
object HivePublisherBuilder {

  def apply(properties: Properties, sc: SparkContext): HiveSchemaPublisher = {

    val hiveDb = properties.getProperty(PluginConfigs.HiveDB._1, PluginConfigs.HiveDB._2)
    val hiveSerdeJar = properties.getProperty(PluginConfigs.HiveSerdeJar._1, PluginConfigs.HiveSerdeJar._2)
    val hiveSerdeClass = properties.getProperty(PluginConfigs.HiveSerdeClass._1, PluginConfigs.HiveSerdeClass._2)
    val blacklist = properties.getProperty(PluginConfigs.HivePublisherBlacklist._1, PluginConfigs.HivePublisherBlacklist._2).split(",").map(_.trim)

    PluginConfigs.validateConfigs(properties, Array(PluginConfigs.HiveSerdeJar._1, PluginConfigs.HiveSerdeClass._1))

    new HiveSchemaPublisher(
      hiveDb,
      hiveSerdeJar,
      hiveSerdeClass,
      blacklist,
      sc
    )
  }
}
