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

import io.trino.tpcds.generator.InventoryGeneratorColumn._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class InventoryRow(
    nullBitMap: Long,
    private val invDateSk: Long,
    private val invItemSk: Long,
    private val invWarehouseSk: Long,
    private val invQuantityOnHand: Int) extends TableRowWithNulls(nullBitMap, INV_DATE_SK)
  with KyuubiTableRowWithNulls {

  override val nullBitMapInternal = nullBitMap
  override val firstColumnInternal = INV_DATE_SK
  def getInvDateSk: Long = invDateSk
  def getInvItemSk: Long = invItemSk
  def getInvWarehouseSk: Long = invWarehouseSk
  def getInvQuantityOnHand: Int = invQuantityOnHand

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
        getLongOrNull(invDateSk, INV_DATE_SK),
        getLongOrNull(invItemSk, INV_ITEM_SK),
        getLongOrNull(invWarehouseSk, INV_WAREHOUSE_SK),
        getIntOrNull(invQuantityOnHand, INV_QUANTITY_ON_HAND)))
}
