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

import java.util.{Calendar, Properties}

import com.groupon.dse.configs.PluginConfigs


/**
 * Case class to store the schema state
 * @param timestamp The access time
 */
case class SchemaState(timestamp: Long)

/**
 * Policy trait that decides when we start publishing
 */
trait StartPolicy extends Serializable{
  def canStart(local: SchemaState): Boolean
}

/**
 * Start policy based on time which decides the time after which we start publishing
 * @param thresholdTime The time to wait before publishing
 * @param startTime The time the schema inferer started
 */
class TimeBasedStartPolicy(thresholdTime: Long, startTime: Long) extends StartPolicy {
  override def canStart(local: SchemaState): Boolean = (local.timestamp - startTime > thresholdTime*1000)
}


/**
 * Helper object to build a TIME based start policy. Can be extended to add more policies
 */
object StartPolicyBuilder {

  def apply(properties: Properties): StartPolicy = {

    val startPolicyType = properties.getProperty(PluginConfigs.TopicStartPolicy._1, PluginConfigs.TopicStartPolicy._2)
    val thresholdTime = properties.getProperty(PluginConfigs.ThresholdTime._1, PluginConfigs.ThresholdTime._2)

    startPolicyType match {
      case "TIME" => new TimeBasedStartPolicy(thresholdTime.toLong, Calendar.getInstance().getTimeInMillis)
      case _ => throw InvalidStartPolicyException("Valid policies are: TIME")
    }
  }

  case class InvalidStartPolicyException(message: String) extends Exception(message)

}