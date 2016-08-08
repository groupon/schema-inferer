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

import com.groupon.dse.schema.Schema
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

/**
 * Converts the schema to a hash map .
 */
object SchemaNormalizer{
  /**
   * Converts a schema(StructType) to a hashmap
   * @return a hashmap representation of the schema inferred by spark
   */
  def normalize(schema: StructType, topic: String): Schema = {
    val hash = scala.collection.mutable.HashMap.empty[String, AnyRef]
    schema.fields.foreach{ field =>
      hash += field.name -> processDataType(field.dataType)
    }
    new Schema(HashMap(hash.toSeq:_*), topic)
  }

  /**
   * Process a field of type structType in the schema
   * @param fields a structType which is a sequence of structFields
   * @return a hashmap representation of the structype
   */
  private def processStructType(fields: Seq[StructField]): HashMap[String, AnyRef] = {
    val hash = scala.collection.mutable.HashMap.empty[String, AnyRef]
    fields.foreach{ field =>
      hash += field.name -> processDataType(field.dataType)
    }
    HashMap(hash.toSeq:_*)
  }

  /**
   * Process a field of type arrayType
   * @param dataType The type of the elements contained in the array
   * @return a list containing the type of the element
   */
  private def processArrayType(dataType: DataType): List[AnyRef] =  {
    val list = ListBuffer[AnyRef]()
    list += processDataType(dataType)
    list.toList
  }

  /**
   * Process a generic datatype which could be an array, struct or a primitive datatype
   * @param dataType The datatype to process
   * @return Any object depending on its datatype
   */
  private def processDataType(dataType: DataType): AnyRef = {
    dataType match {
      case struct: StructType => processStructType(struct.fields)
      case array: ArrayType => processArrayType(array.elementType)
      case _ => dataType.typeName
    }
  }
}
