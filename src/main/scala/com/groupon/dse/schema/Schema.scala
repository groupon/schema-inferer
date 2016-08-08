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

package com.groupon.dse.schema

import com.groupon.dse.commons.SchemaPrettyPrinter
import com.groupon.dse.commons.Utils._
import org.json4s.jackson.Serialization

import scala.collection.immutable.HashMap

/**
 * Generic Schema class on which we can perform schema computations like merge, compare etc
 * @param content The hashmap representation of the schema
 * @param topic The topic of the schema
 */
class Schema(val content: HashMap[String, AnyRef], val topic: String, val parentDiff: HashMap[String, AnyRef] = null) extends Serializable{
  @transient lazy implicit val formats = org.json4s.DefaultFormats

  /**
   * Compare this schema with the schema provided
   * @param schema The schema to compare with
   * @return A map containing the differences if any new fields are present in the new schema, null otherwise
   */
  def diff(schema: Schema) : HashMap[String, AnyRef] = {
    if(schema == null)
      return content
    val SchemaDiff = (schema.content compareMaps content).asInstanceOf[HashMap[String, AnyRef]]
    SchemaDiff
  }

  /**
   * Merge this schema with the schema provided
   * @param schema the schema to merge with
   * @return the Newly merged generic schema object
   */
  def union(schema: Schema): Schema = {
    if(schema == null)
      return new Schema(content, topic, content)
    val mergedMap = (schema.content deepMerge content).asInstanceOf[HashMap[String, AnyRef]]
    val SchemaDiff = this diff schema
    new Schema(mergedMap, topic, SchemaDiff)
  }

  /**
   * @return the string representation of the schema
   */
  def prettyPrint: String = SchemaPrettyPrinter.print(content)

  /**
   * Serialize a map to a json string
   * @return The json representation of the map
   */
  def toJsonString: String = {
    try{
      Serialization.write(content)
    }catch {
      case e: Exception =>
        throw SchemaSerializationException(e.getMessage)
    }
  }
}
case class SchemaSerializationException(message: String) extends Exception(message)
