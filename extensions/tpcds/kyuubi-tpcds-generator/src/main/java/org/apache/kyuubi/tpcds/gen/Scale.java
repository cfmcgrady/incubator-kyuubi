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

/** Cardinalities and business-key domains using native integer-scale interpolation. */
public final class Scale {
  private static final int[] SCALES = {1, 10, 100, 300, 1000, 3000, 10000, 30000, 100000};
  private final double scale;

  public Scale(double scale) {
    if (!Double.isFinite(scale)
        || scale < 0
        || scale > 100000
        || (scale != 0.01 && scale != Math.rint(scale))) {
      throw new IllegalArgumentException("Unsupported TPC-DS scale: " + scale);
    }
    this.scale = scale;
  }

  public double value() {
    return scale;
  }

  public long rows(TpcdsTable table) {
    if (scale == 0) return 0;
    if (table == TpcdsTable.INVENTORY)
      return ids(TpcdsTable.ITEM) * rows(TpcdsTable.WAREHOUSE) * 261;
    table = table.parent();
    long base;
    if (scale == 0.01) {
      base =
          table.scalingModel == 1
              ? table.sizes[0]
              : Math.max(1, Math.round(table.sizes[0] * scale));
    } else {
      int slot = 0;
      while (scale > SCALES[slot]) slot++;
      if (scale == SCALES[slot]) base = table.sizes[slot];
      else if (table.scalingModel == 1) base = table.sizes[0];
      else if (table.scalingModel == 2) base = table.sizes[0] * (long) scale;
      else base = interpolate(table.sizes);
    }
    return base * table.multiplier;
  }

  public long ids(TpcdsTable table) {
    long count = rows(table);
    if (!table.history) return count;
    long remainder = count % 6;
    return count / 6 * 3 + (remainder == 0 ? 0 : remainder == 1 ? 1 : remainder < 4 ? 2 : 3);
  }

  long cities() {
    return pseudo(new long[] {2, 6, 18, 30, 54, 90, 165, 270, 495});
  }

  long counties() {
    return pseudo(new long[] {1, 3, 9, 15, 27, 45, 81, 135, 245});
  }

  long companies() {
    return pseudo(new long[] {2, 3, 4, 5, 5, 5, 5, 5, 5});
  }

  private long pseudo(long[] counts) {
    if (scale < 1) return 1;
    for (int i = 0; i < SCALES.length; i++) if (scale == SCALES[i]) return counts[i];
    return interpolate(counts);
  }

  private long interpolate(long[] counts) {
    int upper = 1;
    while (scale > SCALES[upper]) upper++;
    float fraction = (float) (scale - SCALES[upper - 1]) / (SCALES[upper] - SCALES[upper - 1]);
    return counts[upper - 1] + (int) (fraction * (counts[upper] - counts[upper - 1]));
  }
}
