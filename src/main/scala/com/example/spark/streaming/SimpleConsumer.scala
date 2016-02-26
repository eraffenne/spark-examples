/*
 * spark-examples
 * Copyright (C) 2015 Emmanuelle Raffenne
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see http://www.gnu.org/licenses/.
 */

package com.example.spark.streaming

import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SimpleConsumer {

  def main(args: Array[String]) {

    val master = if ( args.length == 1) args(0) else "local[2]"

    // Create streaming context
    val conf = new SparkConf().setMaster(master).setAppName("SimpleConsumer")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, new Duration(2000))

    // Create a socket stream that listens on localhost:2222
    val stream = ssc.socketTextStream("arwen.local", 2222)

    val parsed = stream.map(t => t.toDouble)
    val result = parsed.reduce(_ + _)

    result.print()
    parsed.print()

    // Start the context
    ssc.start()
    ssc.awaitTermination()
  }

}
