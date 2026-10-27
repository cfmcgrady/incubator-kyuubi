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

/**
 * Reusable typed row; getters are valid only after next() returns true. No TPC-H column is null.
 */
public interface RowCursor {
  boolean next();

  int columnCount();

  /**
   * Integer/key, epoch day for dates, or cents for DOUBLE monetary columns (quantity is also
   * scaled).
   */
  long getLong(int column);

  double getDouble(int column);

  /** Native text representation, including fixed two-place monetary values. */
  String getString(int column);

  /**
   * UTF-8 backing storage for a VARCHAR column. The returned array is read-only and may be shared
   * with other cursors. Only the slice given by getByteOffset/getByteLength is part of the value;
   * consume or copy it before the next next() call. No per-value array or slice object is
   * allocated.
   */
  byte[] getBytes(int column);

  /** Offset into getBytes(column); valid only for VARCHAR columns on the current row. */
  int getByteOffset(int column);

  /** Length in bytes; valid only for VARCHAR columns on the current row. */
  int getByteLength(int column);
}
