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

package org.apache.kyuubi.spark.connector.tpch

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession
import org.apache.kyuubi.tpch.gen.{TpchGenerator, TpchTable}

class TPCHTableSuite extends KyuubiFunSuite {
  private def withSession(f: SparkSession => Unit): Unit = {
    val conf = new SparkConf().setMaster("local[2]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpch", classOf[TPCHCatalog].getName)
    withSparkSession(SparkSession.builder.config(conf).getOrCreate())(f)
  }

  test("all table columns retain typed values at large scales") {
    withSession { spark =>
      Seq(false, true).foreach { ansi =>
        spark.conf.set("spark.connector.tpch.useAnsiStringType", ansi)
        val conf = TPCHConf(spark, CaseInsensitiveStringMap.empty())
        TpchTable.values().foreach { table =>
          val schema = new TPCHTable(table.getTableName, 100000, conf).schema
          assert(schema.fieldNames.toSeq == table.getColumns.map(_.getName).toSeq)
          val reader = new TPCHPartitionReader(table.getTableName, 100000, 3, 1, schema)
          val expected = TpchGenerator.partition(table, 100000, 3, 1)
          try {
            var count = 0
            while (count < 100 && expected.next()) {
              assert(reader.next())
              val row = reader.get()
              schema.fields.zipWithIndex.foreach { case (field, i) =>
                assert(!row.isNullAt(i))
                field.dataType match {
                  case LongType => assert(row.getLong(i) == expected.getLong(i))
                  case IntegerType | DateType => assert(row.getInt(i) == expected.getLong(i))
                  case DoubleType => assert(row.getDouble(i) == expected.getDouble(i))
                  case StringType | _: VarcharType =>
                    assert(row.getUTF8String(i).toString == expected.getString(i))
                  case other => fail(s"Unexpected type $other")
                }
              }
              count += 1
            }
            assert(count > 0)
          } finally reader.close()
        }
      }
    }
  }

  test("partition sizing uses bytes and rounds up") {
    withSession { spark =>
      val table = new TPCHTable("customer", 1, TPCHConf(spark, CaseInsensitiveStringMap.empty()))
      spark.conf.set("spark.connector.tpch.read.maxPartitionBytes", "1m")
      val scan = table.newScanBuilder(CaseInsensitiveStringMap.empty()).build().toBatch
      assert(scan.planInputPartitions().length == 26)
      spark.conf.set("spark.connector.tpch.read.maxPartitionBytes", "0")
      intercept[IllegalArgumentException] {
        table.newScanBuilder(CaseInsensitiveStringMap.empty())
      }
    }
  }
}
