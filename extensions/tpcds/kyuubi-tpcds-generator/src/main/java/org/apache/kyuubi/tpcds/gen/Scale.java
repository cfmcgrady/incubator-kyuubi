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

import java.util.concurrent.atomic.AtomicReferenceArray;

/** Cardinalities and business-key domains using native integer-scale interpolation. */
public final class Scale {
  private static final int[] SCALES = {1, 10, 100, 300, 1000, 3000, 10000, 30000, 100000};
  private static final long[] CITIES = {2, 6, 18, 30, 54, 90, 165, 270, 495};
  private static final long[] COUNTIES = {1, 3, 9, 15, 27, 45, 81, 135, 245};
  private static final long[] COMPANIES = {2, 3, 4, 5, 5, 5, 5, 5, 5};
  private static final TpcdsTable[] TABLES = TpcdsTable.values();
  // Cache only the finite catalog scales; arbitrary user scales do not accumulate globally.
  private static final AtomicReferenceArray<Counts> STANDARD =
      new AtomicReferenceArray<>(SCALES.length + 2);

  private static final class Counts {
    final long[] rows, ids;
    final long cities, counties, companies;

    Counts(long[] rows, long[] ids, long cities, long counties, long companies) {
      this.rows = rows;
      this.ids = ids;
      this.cities = cities;
      this.counties = counties;
      this.companies = companies;
    }
  }

  private final double scale;
  private final long[] rowCounts;
  private final long[] idCounts;
  private final long cities, counties, companies;

  public Scale(double scale) {
    if (!Double.isFinite(scale)
        || scale < 0
        || scale > 100000
        || (scale != 0.01 && scale != Math.rint(scale))) {
      throw new IllegalArgumentException("Unsupported TPC-DS scale: " + scale);
    }
    this.scale = scale;
    int standard = scale == 0 ? 0 : scale == 0.01 ? 1 : -1;
    for (int i = 0; i < SCALES.length; i++) if (scale == SCALES[i]) standard = i + 2;
    Counts counts = standard < 0 ? null : STANDARD.get(standard);
    if (counts == null) {
      counts = calculateCounts();
      if (standard >= 0 && !STANDARD.compareAndSet(standard, null, counts))
        counts = STANDARD.get(standard);
    }
    rowCounts = counts.rows;
    idCounts = counts.ids;
    cities = counts.cities;
    counties = counts.counties;
    companies = counts.companies;
  }

  private Counts calculateCounts() {
    long[] rows = new long[TABLES.length], ids = new long[TABLES.length];
    for (TpcdsTable table : TABLES) {
      if (table == TpcdsTable.INVENTORY) continue;
      long count = calculateRows(table);
      rows[table.ordinal()] = count;
      ids[table.ordinal()] = identifiers(table, count);
    }
    long inventory = ids[TpcdsTable.ITEM.ordinal()] * rows[TpcdsTable.WAREHOUSE.ordinal()] * 261;
    rows[TpcdsTable.INVENTORY.ordinal()] = inventory;
    ids[TpcdsTable.INVENTORY.ordinal()] = inventory;
    return new Counts(rows, ids, pseudo(CITIES), pseudo(COUNTIES), pseudo(COMPANIES));
  }

  public double value() {
    return scale;
  }

  public long rows(TpcdsTable table) {
    return rowCounts[table.ordinal()];
  }

  private long calculateRows(TpcdsTable table) {
    if (scale == 0) return 0;
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
    return idCounts[table.ordinal()];
  }

  private static long identifiers(TpcdsTable table, long count) {
    if (!table.history) return count;
    long remainder = count % 6;
    return count / 6 * 3 + (remainder == 0 ? 0 : remainder == 1 ? 1 : remainder < 4 ? 2 : 3);
  }

  long cities() {
    return cities;
  }

  long counties() {
    return counties;
  }

  long companies() {
    return companies;
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
