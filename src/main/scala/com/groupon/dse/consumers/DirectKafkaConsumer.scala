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

package com.groupon.dse.consumers

import java.util.Properties

import com.groupon.dse.configs.PluginConfigs
import com.groupon.dse.kafka.common.{State, WrappedMessage}
import com.groupon.dse.kafka.controllers.{StateController, StateControllerBuilder}
import com.groupon.dse.spark.plugins.ReceiverPlugin
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.TaskContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.slf4j.LoggerFactory

/**
 * Direct Stream based kafka receiver implementation. Consumes from the max offset and updates the offsets to the zookeeper
 */
class DirectKafkaConsumer(streamingContext: StreamingContext, configs: Properties) extends KafkaConsumer with Serializable{

  val logger = LoggerFactory.getLogger(getClass)
  override def fetchFromKafka( plugins: Seq[ReceiverPlugin]) = {

    //Extract the list of topics
    val topics = configs.getProperty("topics").split(",").map(_.trim)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> configs.getProperty(PluginConfigs.KafkaConnect._1))

    //Create a kafka direct stream
    val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      streamingContext, kafkaParams, topics.toSet)

    //Initialize the state controller and update the state asynchronously
    val stateController = StateControllerBuilder(configs)
    updateZookeeper(messages, stateController, configs)

    var offsetRanges = Array[OffsetRange]()

    //Actual logic to process each RDD and extract the messages per partition
    messages.transform{ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD{ rdd =>
      val wrappedmsg = rdd.mapPartitions{iter =>
        val offsetRange = offsetRanges(TaskContext.get.partitionId)
        var result = List[WrappedMessage]()
        while(iter.hasNext) {
          val value = iter.next()
          result ::= WrappedMessage(offsetRange.topic,offsetRange.partition, "", value._2, offsetRange.untilOffset, offsetRange.untilOffset+1, 0)
        }
        result.toIterator
      }
      //Execute the plugins
      plugins.foreach(plugin => plugin.execute(wrappedmsg))
      //We can also call the state Controller here like Baryon where we only update the state once processing is complete. Something like:
      //stateController.setState(rdd)
      // Dint go with this approach since this was supposedly slowing the processing
    }
  }

  /**
   * Helper to update the zookeeper state
   * @param messages The InputDStream
   * @param stateController The state controller object
   * @param appConfigs The config
   */
  def updateZookeeper(messages: InputDStream[(String, Array[Byte])], stateController: StateController, appConfigs: Properties): Unit = {
    messages.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { _ =>
        val offsetRange = offsetRanges(TaskContext.get.partitionId)
        val topic = offsetRange.topic
        val partitionId = offsetRange.partition
        val zkPath = s"${appConfigs.getProperty("statecontroller.zk.root")}/$topic/$partitionId"
        stateController.setState(zkPath,State(offsetRange.untilOffset+1, System.currentTimeMillis()))
        stateController.close()
      }
    }
  }
}
