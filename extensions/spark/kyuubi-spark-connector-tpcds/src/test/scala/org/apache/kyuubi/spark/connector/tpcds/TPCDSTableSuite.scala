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

package org.apache.kyuubi.spark.connector.tpcds

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession
import org.apache.kyuubi.spark.connector.tpcds.TPCDSConf._
import org.apache.kyuubi.tpcds.gen.{RowRange, TpcdsGenerator, TpcdsTable => Table}

class TPCDSTableSuite extends KyuubiFunSuite {

  Seq(Table.STORE_SALES -> 1176, Table.STORE_RETURNS -> 121).foreach { case (table, nativeCount) =>
    test(s"${table.getName} generation across the 32-bit row number boundary") {
      val sparkConf = new SparkConf().setMaster("local[1]")
        .set("spark.ui.enabled", "false")
        .set("spark.sql.catalogImplementation", "in-memory")
      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        val conf = TPCDSConf(spark, CaseInsensitiveStringMap.empty())
        val schema = new TPCDSTable(table.getName, 10000, conf).schema
        val firstRow = Int.MaxValue.toLong - 2
        val lastRow = firstRow + 20

        def rows(first: Long, last: Long): Vector[InternalRow] = {
          new TPCDSRowCursorIterator(
            TpcdsGenerator.range(table, 10000, new RowRange(first - 1, last)),
            schema)
            .asScala.map(_.copy()).toVector
        }

        // Generate across the boundary without seeking past it, then compare with split reads.
        val expected = rows(firstRow, lastRow)
        assert(expected.nonEmpty)
        Seq(-1, 0, 1, 2, 10).foreach { offset =>
          val splitRow = Int.MaxValue.toLong + offset
          val actual = rows(firstRow, splitRow - 1) ++ rows(splitRow, lastRow)
          assertResult(expected.size, s"split at $splitRow")(actual.size)
          actual.zip(expected).foreach { case (actualRow, expectedRow) =>
            assert(actualRow == expectedRow, s"split at $splitRow")
          }
        }

        // Native dsdgen -scale 10000 -table store_sales -parallel 24000000 -child 23000001
        // generates 100 orders starting at 2300000001, including their store_returns rows.
        val reader = new TPCDSPartitionReader(table.getName, 10000, 24000000, 23000001, schema)
        var count = 0
        try {
          while (reader.next()) {
            count += 1
          }
        } finally {
          reader.close()
        }
        assert(count == nativeCount)
      }
    }
  }

  test("useAnsiStringType (true, false)") {
    Seq(true, false).foreach(key => {
      val sparkConf = new SparkConf().setMaster("local[*]")
        .set("spark.ui.enabled", "false")
        .set("spark.sql.catalogImplementation", "in-memory")
        .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
        .set("spark.sql.catalog.tpcds.useAnsiStringType", key.toString)
      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        val rows = spark.sql("desc tpcds.sf1.call_center").collect()
        rows.foreach(row => {
          val dataType = row.getString(1)
          row.getString(0) match {
            case "cc_call_center_id" =>
              if (key) {
                assert(dataType == "char(16)")
              } else {
                assert(dataType == "string")
              }
            case "cc_name" =>
              if (key) {
                assert(dataType == "varchar(50)")
              } else {
                assert(dataType == "string")
              }
            case _ =>
          }
        })
      }
    })
  }

  test("all table schemas admit generated nulls and support typed row conversion") {
    val sparkConf = new SparkConf().setMaster("local[1]").set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      Table.values().foreach { table =>
        val schema = new TPCDSTable(
          table.getName,
          10000,
          TPCDSConf(spark, CaseInsensitiveStringMap.empty())).schema
        val count = math.min(100L, new org.apache.kyuubi.tpcds.gen.Scale(10000).rows(table))
        val expected = TpcdsGenerator.range(table, 10000, new RowRange(0, count))
        val actual = new TPCDSRowCursorIterator(
          TpcdsGenerator.range(table, 10000, new RowRange(0, count)),
          schema)
        while (expected.next()) {
          assert(actual.hasNext, table.getName)
          val row = actual.next()
          schema.fields.zipWithIndex.foreach { case (field, i) =>
            assert(row.isNullAt(i) == expected.isNull(i), s"${table.getName}.${field.name}")
            if (row.isNullAt(i)) {
              assert(field.nullable, s"${table.getName}.${field.name}")
            } else {
              import org.apache.spark.sql.types._
              field.dataType match {
                case IntegerType => assert(row.getInt(i).toLong == expected.getLong(i))
                case LongType => assert(row.getLong(i) == expected.getLong(i))
                case DateType => assert(row.getInt(i).toLong == expected.getLong(i) - 2440588L)
                case d: DecimalType =>
                  assert(row.getDecimal(
                    i,
                    d.precision,
                    d.scale).toUnscaledLong == expected.getLong(i))
                case _ => assert(row.getUTF8String(i).toString == expected.getString(i))
              }
            }
          }
        }
        assert(!actual.hasNext, table.getName)
      }
    }
  }

  test("maxPartitionBytes rounds up and respects default parallelism") {
    val sparkConf = new SparkConf().setMaster("local[2]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      def assertPartitions(tableName: String, maxBytes: Long, expected: Int): Unit = {
        spark.conf.set(s"$TPCDS_CONNECTOR_READ_CONF_PREFIX.$MAX_PARTITION_BYTES_CONF", maxBytes)
        val df = spark.sql(s"select * from $tableName")
        val scan = df.queryExecution.executedPlan.collectFirst {
          case scanExec: BatchScanExec if scanExec.scan.isInstanceOf[TPCDSBatchScan] =>
            scanExec.scan.asInstanceOf[TPCDSBatchScan]
        }
        assert(scan.isDefined)
        assert(scan.get.planInputPartitions.length == expected)
      }

      val size = TPCDSStatisticsUtils.sizeInBytes(Table.STORE_SALES, 1)
      assertPartitions("tpcds.sf1.store_sales", size / 4, 4)
      assertPartitions("tpcds.sf1.store_sales", size / 4 - 1, 5)
      assertPartitions("tpcds.sf1.store_sales", size / 4 + 1, 4)
      assertPartitions("tpcds.sf1.store_sales", size * 2, 2)
      assertPartitions("tpcds.sf0.store_sales", size, 2)
      assertPartitions("tpcds.sf1.call_center", 1, 1)
    }
  }
}
