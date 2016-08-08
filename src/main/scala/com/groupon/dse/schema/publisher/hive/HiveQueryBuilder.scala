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

import com.groupon.dse.schema.Schema

/**
 * Class to build a hive query given a Schema object. Used by the hive publisher internally

 */
object HiveQueryBuilder {
  private var topLevel = false
  /**
   * Creates the final hive table.
   * @param schema The schema string
   * @param serdeClass The serdeClass to use
   * @return The hive create table query string
   *
   *         TODO: Get serde, serdeClass and location from user. Possibly a config file for all topics to consume from like mr consumer
   */
  def createTableQueryForSchema(schema: Schema, serdeClass: String): String = {
    val builder = new StringBuilder
    builder.append(s"CREATE EXTERNAL TABLE ${schema.topic} (")
    builder.append(transformSchema(schema))
    builder.append(")")
    builder.append(s"PARTITIONED BY (date string, hour string) ROW FORMAT SERDE '$serdeClass' STORED AS sequencefile LOCATION '/user/kafka/source=${schema.topic}'") //TODO
    println(builder.toString())
    builder.toString()
  }


  /**
   * Converts a generic schema into a schema compatible with hive
   * @param mergedSchema The generic schema object
   * @return The string representation of the hive schema
   */
  def transformSchema(mergedSchema: Schema): String = {
    val builder = new StringBuilder
    val map = mergedSchema.content
    for(k <- map.keys){
      topLevel = true
      processField(builder, k, map.get(k))
    }
    builder.deleteCharAt(builder.length - 1)
    builder.toString()
  }

  /**
   * Process a nested map
   * @param builder The string builder to append to
   * @param map The map whose fields need to be processed
   */
  def processMap(builder: StringBuilder,  map: Map[String, Any]){
    builder.append("<")
    if(map.size == 0){
      builder.append("empty: boolean>") //to take care of structs having no fields.
    }else {
      topLevel = false
      for (k <- map.keys) {
        processField(builder, k, map.get(k))
      }
      builder.deleteCharAt(builder.length - 1)
      builder.append(">,")
    }
  }

  /**
   * Processes a field of Any type and appends the field name and type to the stringbuilder
   * @param builder The string builder to append to
   * @param name The name of the field
   * @param dataType The datatype of the field
   */
  def processField(builder: StringBuilder, name: String, dataType: Any) {
    appendToString(builder, processFieldName(name), convertDataType(getDataType(dataType)))
    processDataType(builder, name, dataType)
  }

  /**
   * Process an array in the hashmap
   * @param builder The string builder to append to
   * @param list The array whose type needs to be processed
   */
  def processList(builder: StringBuilder, list: List[Any]){
    builder.append("<")
    val temp = topLevel
    topLevel = true
    processField(builder, "", Some(list(0)))
    builder.deleteCharAt(builder.length - 1)
    builder.append(">,")
    topLevel = temp
  }

  /**
   *  Process a generic datatype which could be an array, struct or a primitive datatype
   * @param builder The string builder to append to
   * @param name The name of the field
   * @param dataType The datatype of the field
   */
  def processDataType(builder: StringBuilder, name: String, dataType: Any){
    dataType match {
      case (Some(v)) =>
        v match {
          case(map: Map[String,_]) => processMap(builder, map)
          case(arr: List[Any]) => processList(builder, arr)
          case(_) => builder.append(",")
        }
      case None => ???
    }
  }

  /**
   * Appends a field: value to a string builder
   * @param builder The string builder to append to
   * @param name The name of the field
   * @param dataType The datatype of the field
   */
  def appendToString(builder: StringBuilder, name: String, dataType: String): Unit = {
    if (topLevel) builder.append(s" $name $dataType")
    else builder.append(s" $name:$dataType")
  }

  /**
   * Convert some datatypes not compatible with hive ( Integer -> int, long -> bigint). Keep rest the same
   * @param dataType The datatype of the field
   * @return The hive compatible datatype
   */
  def convertDataType(dataType: String): String = {
    dataType match {
      case "integer" => "int"
      case "long" => "bigint"
      case _ => dataType
    }
  }

  /**
   * Change field to `field` if field starts with '_' or contains '-' to keep original field name.
   * @param name The field name to process
   * @return The hive compatible field name
   * TODO: make this better
   */
  def processFieldName(name: String): String = {
    //Names containing '_' are not allowed even in ``. So we must change them to be compatible such that the create query doesnt break
    if(name.contains('-'))
      name.replace('-','_')
    else if (name.startsWith("_") || name.contains("."))
      s"`$name`"
    else if (HiveIdentifiers.identifiers.contains(name.toUpperCase))
      s"`$name`"
    else name
  }

  /**
   * Returns the string representation of the datatype.
   * @param datatype The datatype of type Any
   * @return the string representation of the datatype.
   */
  def getDataType(datatype: Any): String = {
    datatype match {
      case (Some(v)) =>
        v match {
          case (map: Map[_, _]) => "struct"
          case (list: List[_]) => "array"
          case(_) => v.toString
        }
      case None => ???
    }
  }
}
