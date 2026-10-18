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

package org.apache.kyuubi.tpcds

import scala.collection.JavaConverters._

import io.trino.tpcds.{Options, Results, Session, Table}
import org.scalatest.funsuite.AnyFunSuite

class TPCDSRowIteratorSuite extends AnyFunSuite {

  private def session(
      table: Table,
      scale: Double,
      parallelism: Int = 1,
      chunk: Int = 1): Session = {
    val options = new Options
    options.table = table.getName
    options.scale = scale
    options.parallelism = parallelism
    options.toSession.withChunkNumber(chunk)
  }

  test("small ranges preserve the original generator's values for all base tables") {
    Table.getBaseTables.asScala.filterNot(_ == Table.DBGEN_VERSION).foreach { table =>
      val conf = session(table, 1)
      val lastRow = math.min(20L, conf.getScaling.getRowCount(table))
      val expected = Results.constructResults(table, 1, lastRow, conf).iterator.asScala
        .map(_.get(0).asScala.toVector).toVector
      val actual = new TPCDSRowIterator(table, conf, 1, lastRow).asScala
        .map(_.asScala.toVector).toVector
      assert(actual == expected, table.getName)
    }
  }

  Seq(Table.STORE_SALES, Table.STORE_RETURNS).foreach { table =>
    test(s"${table.getName} values are independent of splits across Int.MaxValue") {
      val conf = session(table, 10000)
      val firstRow = Int.MaxValue.toLong - 2
      val lastRow = firstRow + 20

      def rows(first: Long, last: Long): Vector[Vector[String]] =
        new TPCDSRowIterator(table, conf, first, last).asScala.map(_.asScala.toVector).toVector

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
    }
  }

  test("large scale partition counts match native dsdgen") {
    // Native dsdgen -scale 10000 -table store_sales -parallel 24000000 -child 23000001.
    // At SF30000 and SF100000, use 100 orders per chunk and select the penultimate chunk.
    // dsdgen writes both the sales and corresponding returns table for each sales command.
    val samples = Seq(
      (10000, Table.STORE_SALES, 24000000, 23000001, 1176),
      (10000, Table.STORE_RETURNS, 24000000, 23000001, 121),
      (30000, Table.STORE_SALES, 72000000, 71999999, 1160),
      (30000, Table.STORE_RETURNS, 72000000, 71999999, 125),
      (30000, Table.CATALOG_SALES, 48000000, 47999999, 863),
      (30000, Table.CATALOG_RETURNS, 48000000, 47999999, 80),
      (30000, Table.WEB_SALES, 18000000, 17999999, 1195),
      (30000, Table.WEB_RETURNS, 18000000, 17999999, 113),
      (100000, Table.STORE_SALES, 240000000, 239999999, 1154),
      (100000, Table.STORE_RETURNS, 240000000, 239999999, 120),
      (100000, Table.CATALOG_SALES, 160000000, 159999999, 893),
      (100000, Table.CATALOG_RETURNS, 160000000, 159999999, 98),
      (100000, Table.WEB_SALES, 60000000, 59999999, 1160),
      (100000, Table.WEB_RETURNS, 60000000, 59999999, 121))
    samples.foreach { case (scale, table, parallelism, chunk, expected) =>
      val conf = session(table, scale, parallelism, chunk)
      assert(
        TPCDSRowIterator(table, conf).asScala.length == expected,
        s"${table.getName} SF$scale chunk $chunk/$parallelism")
    }
  }

  test("non-first chunks of small tables are empty") {
    val table = Table.CALL_CENTER
    val iterator = TPCDSRowIterator(table, session(table, 1, parallelism = 2, chunk = 2))
    assert(!iterator.hasNext)
    intercept[NoSuchElementException](iterator.next())
  }
}
