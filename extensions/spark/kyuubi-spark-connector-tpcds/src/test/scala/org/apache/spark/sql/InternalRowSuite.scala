/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import io.trino.tpcds.`type`.Date
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, RebaseDateTime}
import org.apache.spark.sql.types.{DateType, StructField, StructType}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession

class InternalRowSuite extends KyuubiFunSuite {

  test("aa") {
    //    val d = new Decimal(1234, 3)
    //    println(d)
    //    val d2 = org.apache.spark.sql.types.Decimal(1234L, 200, 3)
    ////    val d2 = org.apache.spark.sql.types.Decimal(100, 8, 2)
    //    println(d2)
    //
    //    println(new Decimal(0, 7))
    //    println(org.apache.spark.sql.types.Decimal(0, 2, 7))
    //    println(org.apache.spark.sql.types.Decimal("0.0000000"))
    //    println(org.apache.spark.sql.types.Decimal(388831L, 7, 2))
    //    println(Long.MaxValue.toString.size)
    //    println(org.apache.spark.sql.types.Decimal(123456L, 7, 2))
    //    println(org.apache.spark.sql.types.Decimal(-5, 5, 2))
    //    println(new Decimal(-5, 5))
    //    println(new Decimal(-5, 2))
    //    println(null.asInstanceOf[Long])
    //    println(org.apache.spark.sql.types.Decimal(-500, 5, 2))
    //    println(org.apache.spark.sql.types.Decimal("-5"))
    //    val ddd = org.apache.spark.sql.types.Decimal(-5)
    //    ddd.changePrecision(5, 2)
    //    println(ddd)
    //    println(org.apache.spark.sql.types.Decimal(""))
    //    println(org.apache.spark.sql.types.Decimal(123456789L, 7, 2))

    import DateTimeUtils._
    Seq(Timestamp.valueOf("1998-01-01 00:00:00.000")).foreach { t =>
      val (d, ns) = toJulianDay(RebaseDateTime.rebaseGregorianToJulianMicros(fromJavaTimestamp(t)))
      println(fromJavaTimestamp(t))
      println(RebaseDateTime.rebaseGregorianToJulianMicros(fromJavaTimestamp(t)))
      println(d)
      println(ns)
      assert(ns > 0)
      println(RebaseDateTime.rebaseJulianToGregorianMicros(fromJulianDay(d, ns)))
      val t1 = toJavaTimestamp(RebaseDateTime.rebaseJulianToGregorianMicros(fromJulianDay(d, ns)))
      assert(t.equals(t1))
    }

    val dateFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val i = 2450815L

    println("...... " + RebaseDateTime.rebaseJulianToGregorianDays(i.toInt))

    val d = Date.fromJulianDays(i.toInt)
    println(d.toString)
    val i2 = LocalDate.parse(d.toString, dateFmt).toEpochDay.toInt
    println(i2)
    //    val i = 2450815L - 2440588
    val r = InternalRow(
      RebaseDateTime.rebaseJulianToGregorianDays(i.toInt) - DateTimeUtils.JULIAN_DAY_OF_EPOCH)
    val schema = StructType(Seq(StructField("d", DateType)))

    println(DateTimeUtils.fromJulianDay(i.toInt, 0L).toInt)

    val sparkConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
    if (true) {
      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        spark.internalCreateDataFrame(spark.sparkContext.makeRDD(Seq(r)), schema)
          .show
      }
    }
  }
}
