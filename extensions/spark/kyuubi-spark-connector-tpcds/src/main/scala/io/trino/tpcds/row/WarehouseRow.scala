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

import java.util.{List => JList}

import io.trino.tpcds.`type`.Address
import io.trino.tpcds.generator.WarehouseGeneratorColumn._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class WarehouseRow(
    nullBitMap: Long,
    private val wWarehouseSk: Long,
    private val wWarehouseId: String,
    private val wWarehouseName: String,
    private val wWarehouseSqFt: Int,
    private val wAddress: Address) extends TableRowWithNulls(nullBitMap, W_WAREHOUSE_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = W_WAREHOUSE_SK

  def getWWarehouseSk: Long = wWarehouseSk
  def getWWarehouseId: String = wWarehouseId
  def getWWarehouseName: String = wWarehouseName
  def getWWarehouseSqFt: Int = wWarehouseSqFt
  def getWAddress: Address = wAddress

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(wWarehouseSk, W_WAREHOUSE_SK),
        getStringOrNullInternal(wWarehouseId, W_WAREHOUSE_ID),
        getStringOrNullInternal(wWarehouseName, W_WAREHOUSE_NAME),
        getIntOrNull(wWarehouseSqFt, W_WAREHOUSE_SQ_FT),
        getStringOrNullInternal(wAddress.getStreetNumber.toString, W_ADDRESS_STREET_NUM),
        getStringOrNullInternal(wAddress.getStreetName, W_ADDRESS_STREET_NAME1),
        getStringOrNullInternal(wAddress.getStreetType, W_ADDRESS_STREET_TYPE),
        getStringOrNullInternal(wAddress.getSuiteNumber, W_ADDRESS_SUITE_NUM),
        getStringOrNullInternal(wAddress.getCity, W_ADDRESS_CITY),
        getStringOrNullInternal(wAddress.getCounty, W_ADDRESS_COUNTY),
        getStringOrNullInternal(wAddress.getState, W_ADDRESS_STATE),
        getStringOrNullInternal("%05d".format(wAddress.getZip), W_ADDRESS_ZIP),
        getStringOrNullInternal(wAddress.getCountry, W_ADDRESS_COUNTRY),
        getDecimalOrNull(wAddress.getGmtOffset, W_ADDRESS_GMT_OFFSET, 5, 2)))
}
