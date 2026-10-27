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

package org.apache.kyuubi.tpch.gen;

/** Zero-based, half-open parent-row range. Native dbgen assigns the remainder to the last chunk. */
public final class RowRange {
  private final long start, end;

  public RowRange(long start, long end) {
    if (start < 0 || end < start) throw new IllegalArgumentException("Invalid row range");
    this.start = start;
    this.end = end;
  }

  public long start() {
    return start;
  }

  public long end() {
    return end;
  }

  public static RowRange partition(TpchTable table, double scale, int partitions, int partition) {
    if (partitions < 1 || partition < 1 || partition > partitions)
      throw new IllegalArgumentException("Invalid partition");
    long rows = table.parentRows(scale);
    if (table.isSmall()) return new RowRange(0, partition == 1 ? rows : 0);
    long each = rows / partitions;
    return new RowRange(each * (partition - 1L), partition == partitions ? rows : each * partition);
  }
}
