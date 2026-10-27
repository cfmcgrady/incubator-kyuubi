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

/** Independent per-column Park-Miller streams with a fixed draw budget per parent row. */
final class RandomStreams {
  private static final long MODULUS = 2147483647L;
  private static final long PERIOD = MODULUS - 1;
  private static final int MULTIPLIER = 16807;
  private static final int DEFAULT_SEED = 19620718;
  private static final int COLUMN_COUNT = 799;
  private static final long[] SMALL_POWERS = powers(256);

  private final int firstColumn;
  private final int[] budgets;
  private final long[] states;
  private final int[] used;

  RandomStreams(int firstColumn, int[] budgets, long rowsToSkip) {
    if (rowsToSkip < 0) {
      throw new IllegalArgumentException("Negative row offset: " + rowsToSkip);
    }
    this.firstColumn = firstColumn;
    this.budgets = budgets.clone();
    states = new long[budgets.length];
    used = new int[budgets.length];
    for (int i = 0; i < budgets.length; i++) {
      if (budgets[i] < 0) {
        throw new IllegalArgumentException("Negative draw budget");
      }
      long seed = DEFAULT_SEED + (firstColumn + i) * (MODULUS / COLUMN_COUNT);
      // Reduce before multiplying: even a Long.MAX_VALUE row offset cannot overflow.
      long draws = ((rowsToSkip % PERIOD) * (budgets[i] % PERIOD)) % PERIOD;
      states[i] = advance(seed, draws);
    }
  }

  long uniform(int column, long minimum, long maximum) {
    long bound = Math.addExact(Math.subtractExact(maximum, minimum), 1);
    if (bound <= 0) {
      throw new IllegalArgumentException("Invalid random range");
    }
    int i = column - firstColumn;
    long state = (states[i] * MULTIPLIER) % MODULUS;
    states[i] = state;
    used[i]++;
    return minimum + state % bound;
  }

  void finishRow() {
    for (int i = 0; i < states.length; i++) {
      // A zero budget denotes initialization-only streams, such as item permutations.
      if (budgets[i] != 0) {
        int remaining = budgets[i] - used[i];
        if (remaining < 0) {
          throw new IllegalStateException("Random stream budget exceeded: " + (firstColumn + i));
        }
        if (remaining > 0) {
          states[i] = advance(states[i], remaining);
        }
      }
      used[i] = 0;
    }
  }

  private static long advance(long seed, long draws) {
    if (draws < SMALL_POWERS.length) {
      return seed * SMALL_POWERS[(int) draws] % MODULUS;
    }
    long factor = MULTIPLIER;
    while (draws != 0) {
      if ((draws & 1) != 0) {
        seed = seed * factor % MODULUS;
      }
      factor = factor * factor % MODULUS;
      draws >>>= 1;
    }
    return seed;
  }

  private static long[] powers(int length) {
    long[] result = new long[length];
    result[0] = 1;
    for (int i = 1; i < length; i++) {
      result[i] = result[i - 1] * MULTIPLIER % MODULUS;
    }
    return result;
  }
}
