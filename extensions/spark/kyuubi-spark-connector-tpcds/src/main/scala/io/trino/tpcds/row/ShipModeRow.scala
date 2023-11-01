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

import io.trino.tpcds.generator.ShipModeGeneratorColumn._

import java.util.{List => JList}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow

class ShipModeRow(
                   nullBitMap: Long,
                   private val smShipModeSk: Long,
                   private val smShipModeId: String,
                   private val smType: String,
                   private val smCode: String,
                   private val smCarrier: String,
                   private val smContract: String
  ) extends TableRowWithNulls(nullBitMap, SM_SHIP_MODE_SK)
  with KyuubiTableRowWithNulls {

    override val nullBitMapInternal = nullBitMap
    override val firstColumnInternal = SM_SHIP_MODE_SK

  def getSmShipModeSk: Long = smShipModeSk
  def getSmShipModeId: String = smShipModeId
  def getSmType: String = smType
  def getSmCode: String = smCode
  def getSmCarrier: String = smCarrier
  def getSmContract: String = smContract

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
    getLongOrNull(smShipModeSk, SM_SHIP_MODE_SK),
    getStringOrNullInternal(smShipModeId, SM_SHIP_MODE_ID),
    getStringOrNullInternal(smType, SM_TYPE),
    getStringOrNullInternal(smCode, SM_CODE),
    getStringOrNullInternal(smCarrier, SM_CARRIER),
    getStringOrNullInternal(smContract, SM_CONTRACT))
  )
}
