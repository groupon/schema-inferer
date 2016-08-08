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

package com.groupon.dse.extractor

import com.groupon.dse.commons.EmbeddedSpark
import com.groupon.dse.kafka.common.WrappedMessage
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
 * Tests for RawBasicExtractor
 */
class RawBasicExtractorTest extends FlatSpec with BeforeAndAfter {
  var extractor: Extractor = _
  var embeddedSpark: EmbeddedSpark = _
  var sc: SparkContext = _

  before {
    extractor = new RawBasicExtractor
    embeddedSpark = new EmbeddedSpark(getClass.getName, 4, 500)
    sc = embeddedSpark.getStreamingContext.sparkContext
  }

  after {
    embeddedSpark.stop
  }

  "The contents of a wrapped message " should "be extracted correctly" in {
    val data = "testdata"
    val msgs = Array(WrappedMessage("testTopic",0,"state",data.getBytes,0,0,0))
    val extracted = extractor.extract(sc.parallelize(msgs))
    assert(extracted.count() == 1)
    assert(extracted.collect()(0).topic == "testTopic")
    assert(extracted.collect()(0).event == "testdata")
  }
}
