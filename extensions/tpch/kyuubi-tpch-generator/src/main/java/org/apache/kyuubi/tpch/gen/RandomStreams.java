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

import static org.apache.kyuubi.tpch.gen.StreamIds.*;

/** Per-cursor random streams and bounded per-parent draw budgets from dbgen 2.17.3. */
final class RandomStreams {
  static final long MODULUS = 2147483647L;
  static final long PERIOD = MODULUS - 1;
  private static final long[] SEEDS = {
    1,
    46831694,
    1841581359,
    1193163244,
    727633698,
    933588178,
    804159733,
    1671059989,
    1051288424,
    1961692154,
    1227283347,
    1171034773,
    276090261,
    1066728069,
    209208115,
    554590007,
    721958466,
    1371272478,
    675466456,
    1808217256,
    2095021727,
    1769349045,
    904914315,
    373135028,
    717419739,
    1095462486,
    881155353,
    1489529863,
    1521138112,
    298370230,
    1140279430,
    1335826707,
    706178559,
    110356601,
    884434366,
    962338209,
    1341315363,
    709314158,
    591449447,
    431918286,
    851767375,
    606179079,
    1500869201,
    1434868289,
    263032577,
    753643799,
    202794285,
    715851524
  };
  private static final int[] BUDGETS = {
    1, 1, 1, 1, 1, 1, 2, 4, 4, 8, 1, 1, 2, 1, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 14, 9, 1, 3, 1, 1, 2,
    9, 1, 3, 1, 2, 92, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1
  };
  private static final long[] PADDING = new long[93];

  static {
    PADDING[0] = 1;
    for (int i = 1; i < PADDING.length; i++) PADDING[i] = PADDING[i - 1] * 16807 % MODULUS;
  }

  private final long[] states = SEEDS.clone();
  private final int[] used = new int[48];
  private final int[] active;

  RandomStreams(TpchTable table, long offset) {
    switch (table) {
      case PART:
        active =
            new int[] {P_MANUFACTURER, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_COMMENT, P_NAME};
        break;
      case PART_SUPPLIER:
        active = new int[] {PS_QUANTITY, PS_COST, PS_COMMENT};
        break;
      case CUSTOMER:
        active = new int[] {C_ADDRESS, C_NATION, C_PHONE, C_BALANCE, C_SEGMENT, C_COMMENT};
        break;
      case SUPPLIER:
        active =
            new int[] {
              S_ADDRESS,
              S_NATION,
              S_PHONE,
              S_BALANCE,
              S_COMMENT,
              S_NOISE,
              S_COMMENT_TYPE,
              S_MARKED_COMMENT,
              S_COMMENT_OFFSET
            };
        break;
      case NATION:
        active = new int[] {N_COMMENT};
        break;
      case REGION:
        active = new int[] {R_COMMENT};
        break;
      case ORDERS:
        active =
            new int[] {
              O_CLERK,
              O_COMMENT,
              O_DATE,
              L_QUANTITY,
              L_DISCOUNT,
              L_TAX,
              L_PART,
              L_SHIP_DATE,
              O_PRIORITY,
              O_CUSTOMER,
              O_LINE_COUNT
            };
        break;
      case LINE_ITEM:
        active =
            new int[] {
              O_DATE,
              L_QUANTITY,
              L_DISCOUNT,
              L_TAX,
              L_INSTRUCTIONS,
              L_MODE,
              L_PART,
              L_SUPPLIER,
              L_SHIP_DATE,
              L_COMMIT_DATE,
              L_RECEIPT_DATE,
              L_RETURN_FLAG,
              L_COMMENT,
              O_LINE_COUNT
            };
        break;
      default:
        throw new IllegalArgumentException("Unsupported table: " + table);
    }
    for (int stream : active)
      states[stream] = advance(states[stream], (offset % PERIOD) * BUDGETS[stream]);
  }

  long uniform(int stream, long low, long high) {
    used[stream]++;
    states[stream] = states[stream] * 16807 % MODULUS;
    return low + (long) (((double) states[stream] / MODULUS) * (high - low + 1));
  }

  long key(int stream, long high, boolean wide) {
    if (!wide) return uniform(stream, 1, high);
    used[stream]++;
    long value = states[stream] * 6364136223846793005L + 1;
    states[stream] = value;
    // Native signed two's-complement arithmetic, compiled with -fwrapv.
    return 1 + Math.abs(value) % high;
  }

  long addressBits(int stream) {
    used[stream]++;
    states[stream] = states[stream] * 16807 % MODULUS;
    // The native MAX_LONG + 1 expression wraps in its int32_t range calculation.
    return (long) (((double) states[stream] / MODULUS) * Integer.MIN_VALUE);
  }

  void finish() {
    for (int stream : active) {
      int remaining = BUDGETS[stream] - used[stream];
      if (remaining < 0) throw new IllegalStateException("Random budget exceeded: " + stream);
      if (remaining != 0) {
        long state = states[stream];
        // A combined multiplier is safe for the 31-bit domain. Mixed 64-bit streams
        // must retain native intermediate wrapping in advance().
        states[stream] =
            state >= 0 && state < MODULUS
                ? state * PADDING[remaining] % MODULUS
                : advance(state, remaining);
      }
      used[stream] = 0;
    }
  }

  static long advance(long state, long draws) {
    if (draws < 0) throw new IllegalArgumentException("Negative random offset");
    long factor = 16807;
    while (draws != 0) {
      if ((draws & 1) != 0) state = state * factor % MODULUS;
      factor = factor * factor % MODULUS;
      draws >>>= 1;
    }
    return state;
  }
}
