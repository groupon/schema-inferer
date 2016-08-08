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

/**
 * Utility object used by other builders to validate if required configs are present.
 */
object PluginConfigs {

  val PublisherType = ("publisher.type", "")
  val StoreType = ("store.type","HASH")
  val ZkConnect = ("store.zk.connect", "")
  val ZkRoot = ("store.zk.root","")
  val Topics = ("topics", "")
  val TopicStartPolicy = ("topic.warmup.policy", "TIME")
  val ThresholdTime = ("topic.warmup.time.sec","60")
  val SparkReceivers = ("spark.num.receivers", "3")
  val HiveDB = ("publisher.hive.db", "schema_inferer_test")
  val HiveSerdeJar = ("publisher.hive.serde.jar", "")
  val HiveSerdeClass = ("publisher.hive.serde.class", "")
  val HivePublisherBlacklist = ("publisher.hive.blacklist", "")
  val ConsumerType = ("consumer.type", "baryon")
  val KafkaConnect = ("metadata.broker.list", "")
  val SamplingRatio = ("rdd.sampling.ratio", "0.25")

  /**
   * Check if the provided [[Properties]] object has the required keys
   *
   * @param properties Provided properties
   * @param requiredProperties Required properties
   */
  @throws[MissingConfigException]
  def validateConfigs(properties: Properties, requiredProperties: Seq[String]): Unit = {
    requiredProperties.foreach(c => {
      if (!properties.containsKey(c)) {
        throw MissingConfigException("Configs missing. Required configs: " + requiredProperties.mkString(", "))
      }
    })
  }
  case class MissingConfigException(message: String) extends Exception(message)
}
