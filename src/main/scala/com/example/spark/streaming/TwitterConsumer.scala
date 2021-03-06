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

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.emmaland.utils.TwitterCredentials

object TwitterConsumer {

    def main(args: Array[String]): Unit = {

        // Set credentials for Twitter
        TwitterCredentials.configure()

        // Setting needed contexts
        val config: SparkConf = new SparkConf().setMaster("local[4]").setAppName("twitterConsumer")
        // -- spark context
        val sc: SparkContext = new SparkContext(config)
        // -- streaming context
        val ssc = new StreamingContext(sc, Seconds(2))

        // Create the stream
        val twitterStream = TwitterUtils.createStream(ssc, None)


        /**
         * Your code should go here
         * */

    }

}
