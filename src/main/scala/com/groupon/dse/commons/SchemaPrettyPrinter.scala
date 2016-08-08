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

/**
 * Prints the nested hashmap
 */
object SchemaPrettyPrinter {
  /**
   * Function returning a well formatted string representation of the hashamp for display
   * @param map the map to convert to string
   * @return the string representation of the map
   */
  def print(map: Map[String, AnyRef]): String = {
    if(map == null)
      return ""
    val builder = new StringBuilder
    builder.append("root\n")
    for(k <- map.keys){
      processDataType(" |", k.toString, map.get(k), builder)
    }
    builder.toString()
  }

  /**
   * Processes the fields of a nested map
   * @param prefix The prefix to append to the field
   * @param name The field name of the nested map
   * @param map The actual map to be processed
   * @param builder The stringbuilder to which the fields are appended.
   */
  private def printMap(prefix: String, name: String, map: Map[String, Any], builder: StringBuilder): Unit ={
    builder.append(s"$prefix-- $name: struct < \n")
    for(k <- map.keys){
      processDataType(prefix +"    |", k.toString, map.get(k), builder)
    }
    builder.append(s"$prefix-- > \n")
  }

  /**
   * Processes an array  nested in a map.
   * @param prefix The prefix to append to the field
   * @param name The field name of the nested array
   * @param list The list to be processed
   * @param builder The stringbuilder to which the fields are appended.
   */
  private def printList(prefix: String, name: String, list: List[Any], builder: StringBuilder){
    builder.append(s"$prefix-- $name: array [\n")
    processDataType(prefix+"   |", "element", Some(list(0)), builder)
    builder.append(s"$prefix-- ] \n")
  }

  /**
   *  Process a generic datatype which could be an array, struct or a primitive datatype
   * @param prefix The prefix to append to the field
   * @param name The name of the field whose datatype is being processed
   * @param datatype The datatype to process
   * @param builder The stringbuilder to which the fields are appended.
   */
  private def processDataType(prefix: String, name: String, datatype: Any, builder: StringBuilder) : Unit = {
    datatype match {
      case (Some(v)) =>
        v match {
          case (map: Map[String, Any]) => printMap(prefix, name, map, builder)
          case (list: List[Any]) => printList(prefix, name, list, builder)
          case (_) => builder.append(s"$prefix-- $name: $v \n")
        }
      case None => ???
    }
  }
}