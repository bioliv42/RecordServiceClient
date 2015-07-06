// Confidential Cloudera Information: Covered by NDA.
// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.recordservice.example

import com.cloudera.recordservice.spark.{RecordServiceContext}
import org.apache.spark._
import org.apache.spark.SparkContext._

/**
 * Example application that does word count on a directory.
 * This demonstrates how the APIs differ. recordServiceTextFile() should be a
 * drop in replacement for textFile()
 */
object WordCount {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("Word Count")
      .setMaster("local")
    var path = "/test-warehouse/tpch.nation/*"
    if (args.length == 1) path = args(0)

    val sc = new SparkContext(sparkConf)

    // Comment out one or the other.
    val file = sc.recordServiceTextFile(path)
    //val file = sc.textFile(path)

    val words = file.flatMap(line => tokenize(line))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    wordCounts.foreach(println)
    sc.stop()
  }

  // Split a piece of text into individual words.
  private def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}
