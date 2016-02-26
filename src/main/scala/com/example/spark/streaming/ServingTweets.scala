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

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.emmaland.utils.TwitterCredentials

object ServingTweets {

    def main(args: Array[String]) {

        // Set credentials for Twitter
        TwitterCredentials.configure()

        // structures
        val schema =
            StructType(Seq(
                new StructField("fecha", StringType, true),
                new StructField("id", LongType, true),
                new StructField("tweet", StringType, true)))

        // Configure job
        val conf = new SparkConf().setAppName("ServingTweets").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(10))
        val hiveContext = new HiveContext(sc)

        HiveThriftServer2.startWithContext(hiveContext)

        val twitterStream = TwitterUtils.createStream(ssc, None)

        twitterStream.foreachRDD { rdd =>
            val rdd2 = rdd.map { tweet =>
                Row(tweet.getCreatedAt.toString, tweet.getId, tweet.getText)
            }
            val df = hiveContext.createDataFrame(rdd2, schema)
            df.registerTempTable("tweets")
        }

        ssc.start()
        ssc.awaitTermination()

    }

}
