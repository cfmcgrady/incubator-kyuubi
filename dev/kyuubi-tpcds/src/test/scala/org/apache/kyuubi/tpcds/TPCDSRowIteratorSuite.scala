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

import java.nio.file.{Files, Path}
import java.util.Comparator

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.tpcds.gen.{TpcdsGenerator, TpcdsTable => Table}

class TPCDSRowIteratorSuite extends AnyFunSuite {

  test("the dev DataFrame and Parquet writer preserve typed values") {
    val directory = Files.createTempDirectory("tpcds-typed-writer")
    val spark = SparkSession.builder().master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalogImplementation", "in-memory")
      .config("spark.sql.warehouse.dir", directory.resolve("warehouse").toUri.toString)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    try {
      val generators = DataGenerator.initTable(spark).map(table => table.name -> table).toMap
      Seq(Table.CUSTOMER, Table.ITEM, Table.DATE_DIM, Table.STORE_SALES).foreach { table =>
        val generator = generators(table.getName)
        generator.setScaleFactor(1)
        generator.setParallelism(2)
        val frame = generator.toDF.limit(128)
        val expected =
          TPCDSRowIterator.typed(table, 1, 2, 1, frame.schema).asScala.take(128).toVector
        val actual = frame.collect().toVector
        assert(actual.size == expected.size)
        assert(actual.toSet == expected.toSet, table.getName)
        val output = directory.resolve(table.getName).toString
        frame.coalesce(1).write.parquet(output)
        val restored = spark.read.schema(frame.schema).parquet(output).collect().toVector
        assert(restored.size == expected.size)
        assert(restored.toSet == expected.toSet, table.getName)
      }
    } finally {
      spark.stop()
      val files = Files.walk(directory)
      try files.sorted(Comparator.reverseOrder[Path]()).forEach(path => Files.delete(path))
      finally files.close()
    }
  }

  test("typed rows preserve all 24 schemas, values and ownership") {
    Table.values().foreach { table =>
      val schema = StructType(table.getColumns.map { column =>
        val sqlType = column.getType
        val dataType = if (sqlType.startsWith("char") || sqlType.startsWith("varchar")) {
          StringType
        } else DataType.fromDDL(sqlType)
        StructField(column.getName, dataType, column.isNullable)
      })
      val parallelism =
        if (table.getName.endsWith("_sales") ||
          table.getName.endsWith("_returns")) 100000000
        else 1
      val partition = if (parallelism == 1) 1 else parallelism - 1
      val rows = TPCDSRowIterator.typed(table, 100000, parallelism, partition, schema)
      val expected = TpcdsGenerator.partition(table, 100000, parallelism, partition)
      val retained =
        scala.collection.mutable.ArrayBuffer.empty[(org.apache.spark.sql.Row, Vector[Any])]
      var count = 0
      while (count < 100 && expected.next()) {
        assert(rows.hasNext)
        assert(rows.hasNext)
        val row = rows.next()
        schema.fields.zipWithIndex.foreach { case (field, i) =>
          assert(row.isNullAt(i) == expected.isNull(i))
          if (!expected.isNull(i)) {
            field.dataType match {
              case LongType => assert(row.getLong(i) == expected.getLong(i))
              case IntegerType => assert(row.getInt(i) == expected.getLong(i))
              case DateType => assert(row.getDate(i).toString == expected.getString(i))
              case decimal: DecimalType =>
                assert(row.getDecimal(i) == java.math.BigDecimal.valueOf(
                  expected.getLong(i),
                  decimal.scale))
              case StringType => assert(row.getString(i) == expected.getString(i))
              case other => fail(s"Unexpected type: $other")
            }
          }
        }
        retained += row -> row.toSeq.toVector
        count += 1
      }
      assert(count > 0, table.getName)
      retained.foreach { case (row, snapshot) => assert(row.toSeq == snapshot) }
    }
  }

  test("all 24 tables use the text cursor and preserve repeated hasNext calls") {
    Table.values().foreach { table =>
      val iterator = TPCDSRowIterator(table, 1, 1, 1)
      assert(iterator.hasNext, table.getName)
      assert(iterator.hasNext, table.getName)
      val first = iterator.next()
      assert(first.size() == table.columnCount())
      val saved = first.asScala.toVector
      if (iterator.hasNext) iterator.next()
      assert(first.asScala.toVector == saved, "text rows must not alias the reusable cursor")
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
      assert(
        TPCDSRowIterator(table, scale, parallelism, chunk).asScala.length == expected,
        s"${table.getName} SF$scale chunk $chunk/$parallelism")
    }
  }

  test("non-first chunks of small tables are empty") {
    val table = Table.CALL_CENTER
    val iterator = TPCDSRowIterator(table, 1, 2, 2)
    assert(!iterator.hasNext)
    intercept[NoSuchElementException](iterator.next())
  }

  test("nonstandard scales use native customer cardinalities") {
    val first = TPCDSRowIterator(Table.CUSTOMER, 20, 2, 1).asScala
    assert(first.length == 666000)
  }
}
