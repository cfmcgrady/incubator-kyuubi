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

package io.trino.tpcds.row

import io.trino.tpcds.generator.IncomeBandGeneratorColumn._

import java.util.{List => JList}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow

class IncomeBandRow(
                     nullBitMap: Long,
                     private val ibIncomeBandId: Int,
                     private val ibLowerBound: Int,
                     private val ibUpperBound: Int
  ) extends TableRowWithNulls(nullBitMap, IB_INCOME_BAND_ID)
  with KyuubiTableRowWithNulls {

    override val nullBitMapInternal = nullBitMap
    override val firstColumnInternal = IB_INCOME_BAND_ID

  def getIbIncomeBandId: Int = ibIncomeBandId
  def getIbLowerBound: Int = ibLowerBound
  def getIbUpperBound: Int = ibUpperBound

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(ibIncomeBandId, IB_INCOME_BAND_ID),
        getIntOrNull(ibLowerBound, IB_LOWER_BOUND),
        getIntOrNull(ibUpperBound, IB_UPPER_BOUND)))
}
