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

import io.trino.tpcds.generator.ReasonGeneratorColumn._
import java.util.{List => JList}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow

class ReasonRow(
                 nullBitMap: Long,
                 private val rReasonSk: Long,
                 private val rReasonId: String,
                 private val rReasonDescription: String
  ) extends TableRowWithNulls(nullBitMap, R_REASON_SK)
  with KyuubiTableRowWithNulls {

    override val nullBitMapInternal = nullBitMap
    override val firstColumnInternal = R_REASON_SK

  def getrReasonSk: Long = rReasonSk
  def getrReasonId: String = rReasonId
  def getrReasonDescription: String = rReasonDescription

  override def getValues: JList[String] = throw new UnsupportedOperationException()

  def internalRow: InternalRow =
    new GenericInternalRow(
      Array(
    getLongOrNull(rReasonSk, R_REASON_SK),
    getStringOrNullInternal(rReasonId, R_REASON_ID),
    getStringOrNullInternal(rReasonDescription, R_REASON_DESCRIPTION)))
}
