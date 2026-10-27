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

package org.apache.spark.kyuubi

import scala.concurrent.duration._

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.types._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.tpch.TPCHPartitionReader
import org.apache.kyuubi.tpch.gen.TpchTable

/** Run with RUN_BENCHMARK=1 and this suite's wildcardSuites name. */
class TPCHCoreGenerateBenchmark extends KyuubiFunSuite {
  private val scale = 10000

  TpchTable.values().foreach { table =>
    test(s"${table.getTableName} generation") {
      assume(sys.env.contains("RUN_BENCHMARK"))
      val count = math.min(1000000L, table.parentRows(scale)).toInt
      val schema = StructType(table.getColumns.map { column =>
        val dataType = column.getKind match {
          case TpchTable.Kind.INTEGER => IntegerType
          case TpchTable.Kind.IDENTIFIER => LongType
          case TpchTable.Kind.DOUBLE => DoubleType
          case TpchTable.Kind.DATE => DateType
          case TpchTable.Kind.VARCHAR => StringType
        }
        StructField(column.getName, dataType)
      })
      def run(): Long = {
        val reader = new TPCHPartitionReader(table.getTableName, scale, 1, 1, schema)
        var rows = 0
        var checksum = 0L
        try {
          while (rows < count && reader.next()) {
            checksum += reader.get().hashCode()
            rows += 1
          }
        } finally {
          reader.close()
        }
        assert(rows == count)
        checksum
      }
      val expected = run()
      (1 to 3).foreach { _ => assert(run() == expected) }
      val benchmark = new Benchmark(s"${table.getTableName} SF$scale", count, minTime = 1.second)
      benchmark.addCase("Independent core + UTF-8 UnsafeRow", 7) { _ =>
        assert(run() == expected)
      }
      benchmark.run()
    }
  }
}
