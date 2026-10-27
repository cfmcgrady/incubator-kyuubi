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

import com.google.common.collect.AbstractIterator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.kyuubi.tpcds.gen.RowCursor

private[tpcds] class TPCDSRowCursorIterator(generator: RowCursor, schema: StructType)
  extends AbstractIterator[InternalRow] {
  private val writer = new UnsafeRowWriter(generator.columnCount())
  private val types = schema.fields.map(_.dataType)

  override protected def computeNext(): InternalRow = {
    if (!generator.next()) return endOfData()
    writer.reset()
    writer.zeroOutNullBytes()
    var i = 0
    while (i < generator.columnCount()) {
      if (generator.isNull(i)) {
        writer.setNullAt(i)
      } else {
        types(i) match {
          case IntegerType => writer.write(i, Math.toIntExact(generator.getLong(i)))
          case DateType => writer.write(i, Math.toIntExact(generator.getLong(i) - 2440588L))
          case LongType | _: DecimalType => writer.write(i, generator.getLong(i))
          case StringType | _: CharType | _: VarcharType =>
            writer.write(i, UTF8String.fromString(generator.getString(i)))
          case unsupported =>
            throw new IllegalArgumentException(s"Unsupported TPC-DS type: $unsupported")
        }
      }
      i += 1
    }
    writer.getRow
  }
}
