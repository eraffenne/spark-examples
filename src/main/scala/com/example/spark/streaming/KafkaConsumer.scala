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

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object KafkaConsumer extends App {

  override def main(args: Array[String]) {

    val master = if ( args.length == 1) args(0) else "local[2]"

    val config: SparkConf = new SparkConf().setMaster(master).setAppName("KafkaConsumer")
    val sc: SparkContext = new SparkContext(config)
    val ssc: StreamingContext = new StreamingContext(sc, new Duration(2000))

    val quorum = "localhost:2181"
    val group = "Consumers"
    val topics = Map("test" -> 1)

    val stream = KafkaUtils.createStream(ssc, quorum, group, topics)

    val pairs = stream.map( t => (t._1, 1))
    val result = pairs.reduceByKey(_ + _)

    stream.print()
    result.print()

    stream.saveAsTextFiles("file:///tmp/kafka", "stream")

    ssc.start()
    ssc.awaitTermination()

  }

}
