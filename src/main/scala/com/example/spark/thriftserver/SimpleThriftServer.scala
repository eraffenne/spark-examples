/*
 *  spark-examples
 *  Copyright (C) 2015 Emmanuelle Raffenne
 *
 *  This program is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as
 *  published by the Free Software Foundation, either version 3 of
 *  the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program. If not, see http://www.gnu.org/licenses/.
 *
 */

package com.example.spark.thriftserver

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.{Logging, SparkConf, SparkContext}

object SimpleThriftServer extends Logging {

    def main(args: Array[String]) {
        val conf = new SparkConf()
                .setAppName("SimpleThriftServer")
                .setMaster("local[4]")
        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)
        import hiveContext.implicits._

        HiveThriftServer2.startWithContext(hiveContext)

        val rdd = sc.parallelize(List(1, 2, 3, 4, 5))
        val df = rdd.toDF("numero")
        df.registerTempTable("tabla")

        log.info("\n============== hiveContext waiting....\n")
        log.info("Use !connect jdbc:hive2://localhost:10000")

        // Esperar conexiones
        hiveContext.wait()
    }
}
