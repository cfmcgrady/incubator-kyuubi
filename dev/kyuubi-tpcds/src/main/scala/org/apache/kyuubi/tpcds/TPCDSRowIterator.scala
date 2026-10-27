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

import java.util.{ArrayList => JArrayList, Iterator => JIterator, List => JList}

import com.google.common.collect.AbstractIterator

import org.apache.kyuubi.tpcds.gen.{TpcdsGenerator, TpcdsTable}

private[tpcds] object TPCDSRowIterator {
  def apply(
      table: TpcdsTable,
      scale: Double,
      parallelism: Int,
      partition: Int): JIterator[JList[String]] = {
    val generator = TpcdsGenerator.partition(table, scale, parallelism, partition)
    new AbstractIterator[JList[String]] {
      override protected def computeNext(): JList[String] = {
        if (!generator.next()) return endOfData()
        val row = new JArrayList[String](generator.columnCount())
        var i = 0
        while (i < generator.columnCount()) {
          row.add(generator.getString(i))
          i += 1
        }
        row
      }
    }
  }
}
