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
import org.apache.kyuubi.spark.connector.tpcds.TPCDSPartitionReader
import org.apache.kyuubi.tpcds.gen.{Scale, TpcdsTable}

/** Run with RUN_BENCHMARK=1 and this suite's wildcardSuites name. */
class TPCDSCoreGenerateBenchmark extends KyuubiFunSuite {
  private val scale = 10000

  TpcdsTable.values().foreach { table =>
    test(s"${table.getName} generation") {
      assume(sys.env.contains("RUN_BENCHMARK"))
      val count = math.min(100000L, new Scale(scale).rows(table)).toInt
      val schema = StructType(table.getColumns.map { column =>
        val sqlType = column.getType
        val dataType = if (sqlType.startsWith("char") || sqlType.startsWith("varchar")) {
          StringType
        } else DataType.fromDDL(sqlType)
        StructField(column.getName, dataType, column.isNullable)
      })
      def run(): Long = {
        val reader = new TPCDSPartitionReader(table.getName, scale, 1, 1, schema)
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
      val benchmark = new Benchmark(s"${table.getName} SF$scale", count, minTime = 1.second)
      benchmark.addCase("Independent core + UnsafeRow", 3) { _ => assert(run() == expected) }
      benchmark.run()
    }
  }
}
