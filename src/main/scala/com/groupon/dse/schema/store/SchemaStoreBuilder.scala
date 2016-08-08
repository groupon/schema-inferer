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

import java.util.Properties

import com.groupon.dse.configs.PluginConfigs

/**
 * Helper object to build a Hash or zk cache. Default is hash based cache
 */
object SchemaStoreBuilder {

  def apply(properties: Properties): SchemaStore = {

    val storeType = properties.getProperty(PluginConfigs.StoreType._1, PluginConfigs.StoreType._2)
    val zkConnect = properties.getProperty(PluginConfigs.ZkConnect._1, PluginConfigs.ZkConnect._2)
    val zkRoot = properties.getProperty(PluginConfigs.ZkRoot._1, PluginConfigs.ZkRoot._2)
    val zkStoreTime = properties.getProperty(PluginConfigs.ThresholdTime._1, PluginConfigs.ThresholdTime._2)

    storeType match {
      case "ZOOKEEPER" => {
        PluginConfigs.validateConfigs(properties, Array(PluginConfigs.ZkConnect._1, PluginConfigs.ZkRoot._1))
        new ZookeeperSchemaStore(
          zkConnect,
          zkRoot,
          zkStoreTime.toLong
        )
      }
      case "HASH" => new InMemorySchemaStore
      case _ => throw InvalidStoreTypeException("Valid Store types are: HASH, ZOOKEEPER")
    }
  }

  case class InvalidStoreTypeException(message: String) extends Exception(message)
}
