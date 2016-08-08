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

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

/**
 * Utility class that helps to compare/merge datatypes. Helps to compute the union/diff of 2 schemas(hashmaps)
 */
object Utils {
  //Not sure if this is a good way to do this
  var dataTypes = Array("struct", "list","string", "double", "float", "long", "integer", "short", "byte", "boolean")

  implicit class MergableMap[K](map: Map[K, _]) {
    /**
     * Recursive function to merge 2 maps into 1.
     * Incase of a key clash, it chooses the key having the higher precedence based on the "dataTypes" data structure.
     * @param that the map to merge with
     * @return the merged map
     */
    def deepMerge(that: Map[K, _]): Map[K, _] =
      (for (k <- map.keys ++ that.keys) yield {
        val newValue =
          (map.get(k), that.get(k)) match {
            case (Some(v), None) => v
            case (None, Some(v)) => v
            case (Some(v1), Some(v2)) =>
              (v1, v2) match {
                case(m1: Map[K,_], m2: Map[K, _]) => m1 deepMerge m2
                case(_,_) => mergeValues(v1, v2)
              }
            case (_, _) => ???
          }
        k -> newValue
      }).toMap
  }


  implicit class MergeableList[K](v1: List[K]){
    /**
     * Recursive function to merge 2 lists
     * @param v2 The new list to merge with
     * @return the merged list
     */
    def mergeList(v2: List[K]): Any ={
      val val1 = v1(0)
      val val2 = v2(0)
      var changes = ListBuffer[Any]()

      (val1, val2) match {
        case(m1: Map[K,_], m2: Map[K, _]) => {
          changes += (m1 deepMerge m2)
        }
        case(_,_) => changes += mergeValues(val1, val2)
      }
      changes.toList
    }
  }


  implicit class ComparableMap[K](map: Map[K, _]){
    /**
     * Compare 2 maps for equality ie checks if both maps have the same keys. Returns a list of the keys present in the new map
     * and absent from the previous map ie the difference in the 2 maps
     * @param that The new map to compare with
     * @return a list of the keys present in the new map and absent in the previous map
     */
    def compareMaps(that: Map[K, _]): HashMap[String, Any] = {

      var changes = HashMap[String, Any]()

      for (k <- map.keys ++ that.keys) {
        (map.get(k), that.get(k)) match {
          case (Some(v), None) =>
          case (None, Some(v)) => changes += k.toString -> v
          case (Some(v1), Some(v2)) =>
            (v1, v2) match {

              case(m1: Map[K,_], m2: Map[K, _]) => {
                var diff = m1 compareMaps m2
                if (diff != null)
                  changes += k.toString -> diff
              }
              case(arr1: List[K], arr2: List[K]) => {
                val diff = arr1 compareLists (arr2)
                if(diff != null)
                  changes += k.toString -> diff
              }
              case(_,_) => {
               val comp = compareValues(v1, v2)
               if(v2.toString == comp.toString && v1.toString != comp.toString)
                 changes += k.toString -> s"${v2.toString}  -- ${v1.toString}"
              }
            }
          case (_, _) => ???
        }
      }
      if (changes.size > 0)
        changes
      else null
    }
  }

  implicit class ComparableList[K](v1: List[K]){
    /**
     * Compares 2 lists for equality ie checks if both lists have same type of elements.
     * Here the first element in the list stores the type of the list. It gets this from the Schema Normalizer stage
     * @param v2 The new list to compare with
     * @return a list representing the difference in the elements of the 2 lists
     */
    def compareLists(v2: List[K]): Any ={
      val val1 = v1(0)
      val val2 = v2(0)
      var changes = ListBuffer[Any]()

      (val1, val2) match {
        case(m1: Map[K,_], m2: Map[K, _]) => {
          var diff = m1 compareMaps m2
          if(diff != null)
            changes += diff
        }
        case(arr1: List[K], arr2: List[K]) => {
          var diff = arr1 compareLists(arr2)
          if(diff != null)
            changes += diff
        }
        case(_,_) => {
          val comp = compareValues(val1, val2)
          if(val2.toString == comp.toString && val1.toString != comp.toString)
            changes += val1 +" -- "+val2
        }
      }
      if (changes.length > 0)
        changes.toList
      else null
    }
  }

  /**
   * Method to return the preferred key incase of 2 keys having the same name at the same level.
   * This gets the index of both the key types from the "dataTypes" array and chooses the one having the lower index indicating the precedence.
   * @param v1 the first key type
   * @param v2 the second key type
   * @return the key type to choose based on precedence
   */
  def compareValues(v1: Any, v2: Any): Any = {
    val val1 = getVal(v1)
    val val2 = getVal(v2)

    // If both keys are lists, then we recursively match
    if(val1 == 1 && val2 == 1){
      val comp =
        (v1, v2) match {
        case(arr1: List[Any], arr2: List[Any]) => arr1 compareLists arr2
        }
      if(comp != null) return v2
      else return v1
    }
    if(val1 == -1)
      return v2
    else if(val2 == -1 || val1 <= val2)
      return v1
    v2
  }


  /**
   * Method to return the preferred key incase of 2 keys having the same name at the same level.
   * This gets the index of both the key types from the "dataTypes" array and chooses the one having the lower index indicating the precedence.
   * @param v1 the first key type
   * @param v2 the second key type
   * @return the key type to choose based on precedence
   */
  def mergeValues(v1: Any, v2: Any): Any = {
    val val1 = getVal(v1)
    val val2 = getVal(v2)

    // If both keys are lists, then we recursively match
    if(val1 == 1 && val2 == 1){
      val comp =
        (v1, v2) match {
          case(l1: List[Any], l2: List[Any]) => l1 mergeList(l2)
        }
      return comp
    }
    if(val1 == -1)
      return v2
    else if(val2 == -1 || val1 <= val2)
      return v1
    v2
  }

  def getVal(v1: Any) = v1 match {
    case map: Map[_,_] => dataTypes.indexOf("struct")
    case list: List[_] => dataTypes.indexOf("list")
    case _ => dataTypes.indexOf(v1.toString)
  }

}
