/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.tpcds.gen;

import java.time.LocalDate;

abstract class GeneratedRow implements RowCursor {
  final TpcdsTable table;
  final long[] numbers;
  final String[] strings;
  long nulls;
  boolean positioned;

  GeneratedRow(TpcdsTable table) {
    this.table = table;
    numbers = new long[table.columnCount()];
    strings = new String[numbers.length];
  }

  public final int columnCount() {
    return numbers.length;
  }

  private void checkPosition() {
    if (!positioned) throw new IllegalStateException("Call next() before reading the current row");
  }

  public final boolean isNull(int ordinal) {
    checkPosition();
    return (nulls & (1L << ordinal)) != 0;
  }

  public final long getLong(int ordinal) {
    checkPosition();
    return strings[ordinal] == null ? numbers[ordinal] : Long.parseLong(strings[ordinal]);
  }

  public final String getString(int ordinal) {
    if (isNull(ordinal)) return null;
    if (strings[ordinal] != null) return strings[ordinal];
    long value = numbers[ordinal];
    TpcdsTable.Column column = table.column(ordinal);
    if (column.printKind.equals("boolean")) return value == 0 ? "N" : "Y";
    if (column.getType().equals("date")) return LocalDate.ofEpochDay(value - 2440588).toString();
    if (column.getType().startsWith("decimal")) {
      if (column.printKind.equals("integer")) return Long.toString(value / 100);
      return decimal(value);
    }
    return Long.toString(value);
  }

  static String decimal(long value) {
    long magnitude = Math.abs(value), cents = magnitude % 100;
    return (value < 0 ? "-" : "") + magnitude / 100 + "." + (cents < 10 ? "0" : "") + cents;
  }

  final void put(int column, long value) {
    numbers[column] = value;
    strings[column] = null;
  }

  final void put(int column, String value) {
    strings[column] = value;
    numbers[column] = 0;
  }

  final void applyNulls(long bitmap) {
    nulls = 0;
    for (int i = 0; i < numbers.length; i++) {
      TpcdsTable.Column column = table.column(i);
      if ((bitmap & (1L << column.nullBit)) != 0
          || ((column.printKind.equals("key") || column.printKind.equals("date"))
              && numbers[i] == -1
              && strings[i] == null)) {
        nulls |= 1L << i;
      }
    }
  }
}
