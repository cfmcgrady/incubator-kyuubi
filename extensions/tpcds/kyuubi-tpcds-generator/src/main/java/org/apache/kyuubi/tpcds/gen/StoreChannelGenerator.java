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

/**
 * Generates store_sales or store_returns using primitive storage and independent random streams.
 * Values remain valid until the next call to {@link #next()}. Monetary values are unscaled cents.
 * The compatibility reference is databricks/tpcds-kit at 1b7fb7529edae091684201fab142d956d6afd881.
 */
public final class StoreChannelGenerator implements RowCursor {
  // Stream identifiers are part of the pinned reference data format.
  private static final int SOLD_DATE_STREAM = 314;
  private static final int SOLD_TIME_STREAM = 315;
  private static final int SOLD_ITEM_STREAM = 316;
  private static final int SOLD_CUSTOMER_STREAM = 317;
  private static final int SOLD_DEMOGRAPHICS_STREAM = 318;
  private static final int SOLD_HOUSEHOLD_STREAM = 319;
  private static final int SOLD_ADDRESS_STREAM = 320;
  private static final int SOLD_STORE_STREAM = 321;
  private static final int SOLD_PROMOTION_STREAM = 322;
  private static final int TICKET_NUMBER_STREAM = 323;
  private static final int RETURNED_DATE_STREAM = 292;
  private static final int RETURNED_TIME_STREAM = 293;
  private static final int RETURN_CUSTOMER_STREAM = 295;
  private static final int RETURN_DEMOGRAPHICS_STREAM = 296;
  private static final int RETURN_HOUSEHOLD_STREAM = 297;
  private static final int RETURN_ADDRESS_STREAM = 298;
  private static final int RETURN_STORE_STREAM = 299;
  private static final int RETURN_REASON_STREAM = 300;
  private static final int RETURN_CUSTOMER_CHOICE_STREAM = 301;

  private static final int SALES_FIRST_COLUMN = 314;
  private static final int RETURNS_FIRST_COLUMN = 292;
  private static final int SALES_PRICING = 337;
  private static final int RETURNS_PRICING = 312;
  private static final int SALES_NULLS = 338;
  private static final int RETURNS_NULLS = 313;
  private static final int PERMUTATION = 339;
  private static final int RETURN_FLAG = 336;
  private static final int[] SALES_BUDGETS = {
    2, 2, 1, 1, 1, 1, 1, 1, 16, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 128, 32, 0
  };
  private static final int[] RETURNS_BUDGETS = {
    32, 32, 16, 16, 16, 16, 16, 16, 16, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 80, 32
  };
  private static final int[] SALES_NULL_BITS = {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 14, 19, 20, 21
  };
  private static final int[] HOUR_WEIGHTS = {4, 8, 12, 7, 8, 9, 12, 4, 4, 12, 8, 8, 4};
  private static final int[] HOURS = cumulative(HOUR_WEIGHTS);
  private static final int[] ORDINARY_DAYS = calendar(false);
  private static final int[] LEAP_DAYS = calendar(true);
  private static final long JULIAN_EPOCH = 2440588;
  private static final long FIRST_DATE = julian(1998, 1, 1);
  private static final long LAST_DATE = julian(2003, 12, 31);
  private static final long HALF_DATE = FIRST_DATE + (LAST_DATE - FIRST_DATE) / 2;
  private static final long THIRD_DATE = FIRST_DATE + (LAST_DATE - FIRST_DATE) / 3;
  private static final long TWO_THIRDS_DATE = THIRD_DATE + (LAST_DATE - FIRST_DATE) / 3;
  private static final long TODAY = julian(2003, 1, 8);
  private static final long[] YEAR_STARTS = {
    julian(1998, 1, 1),
    julian(1999, 1, 1),
    julian(2000, 1, 1),
    julian(2001, 1, 1),
    julian(2002, 1, 1)
  };

  private final boolean returns;
  private final StoreScale scale;
  private final RowRange range;
  private final RandomStreams salesRandom;
  private final RandomStreams returnsRandom;
  private final int[] items;
  private final long[] values;
  private long nulls;
  private long order;
  private int remaining;
  private int itemPosition;
  private boolean positioned;

  private long soldDate;
  private long soldTime;
  private long customer;
  private long demographics;
  private long household;
  private long address;
  private long store;
  private long item;
  private long promotion;
  private long quantity;
  private long wholesale;
  private long listPrice;
  private long salesPrice;
  private long coupon;
  private long paid;
  private long taxRate;
  private long tax;

  public StoreChannelGenerator(boolean returns, double scale, RowRange range) {
    this.returns = returns;
    this.scale = new StoreScale(scale);
    if (range.end() > this.scale.orders) {
      throw new IllegalArgumentException("Range exceeds the store order count");
    }
    this.range = range;
    order = range.start() + 1;
    salesRandom = new RandomStreams(SALES_FIRST_COLUMN, SALES_BUDGETS, range.start());
    returnsRandom = new RandomStreams(RETURNS_FIRST_COLUMN, RETURNS_BUDGETS, range.start());
    values = new long[returns ? 20 : 23];
    items = new int[range.start() == range.end() ? 0 : this.scale.itemIds];
    for (int i = 0; i < items.length; i++) {
      items[i] = i + 1;
    }
    for (int i = 0; i < items.length; i++) {
      int other = (int) salesRandom.uniform(PERMUTATION, 0, items.length - 1);
      int value = items[i];
      items[i] = items[other];
      items[other] = value;
    }
  }

  public static StoreChannelGenerator partition(
      boolean returns, double scale, int partitions, int partition) {
    StoreScale size = new StoreScale(scale);
    return new StoreChannelGenerator(
        returns, scale, RowRange.split(size.orders, partitions, partition));
  }

  public boolean next() {
    positioned = false;
    while (order <= range.end()) {
      if (remaining == 0) {
        startOrder();
      }
      long salesNulls = nullBitmap(salesRandom, SALES_NULLS, 900);
      itemPosition = (itemPosition + 1) % items.length;
      item = historyKey(items[itemPosition], soldDate, scale.items);
      promotion = salesRandom.uniform(SOLD_PROMOTION_STREAM, 1, scale.promotions);
      priceSale();
      boolean returned = salesRandom.uniform(RETURN_FLAG, 0, 99) < 10;
      boolean emit = !returns || returned;
      if (emit) {
        if (returns) {
          writeReturn();
        } else {
          writeSale(salesNulls);
        }
      }
      if (--remaining == 0) {
        salesRandom.finishRow();
        returnsRandom.finishRow();
        order++;
      }
      if (emit) {
        positioned = true;
        return true;
      }
    }
    return false;
  }

  public int columnCount() {
    return values.length;
  }

  public boolean isNull(int ordinal) {
    requirePosition();
    return (nulls & (1L << ordinal)) != 0;
  }

  public long getLong(int ordinal) {
    requirePosition();
    return values[ordinal];
  }

  /** Formats the current row for the text generator and reference tests. */
  public String getString(int ordinal) {
    if (isNull(ordinal)) {
      return null;
    }
    long value = values[ordinal];
    if (ordinal <= 10) {
      return Long.toString(value);
    }
    long magnitude = Math.abs(value);
    long cents = magnitude % 100;
    return (value < 0 ? "-" : "") + magnitude / 100 + "." + (cents < 10 ? "0" : "") + cents;
  }

  private void requirePosition() {
    if (!positioned) {
      throw new IllegalStateException("Call next() before reading the current row");
    }
  }

  private void startOrder() {
    store = historyKey(salesRandom.uniform(SOLD_STORE_STREAM, 1, scale.storeIds), 1, scale.stores);
    soldTime =
        (8 + pick(HOURS, salesRandom, SOLD_TIME_STREAM)) * 3600L
            + salesRandom.uniform(SOLD_TIME_STREAM, 0, 3599);
    int year = (int) salesRandom.uniform(SOLD_DATE_STREAM, 0, 4);
    soldDate =
        YEAR_STARTS[year]
            + pick(year == 2 ? LEAP_DAYS : ORDINARY_DAYS, salesRandom, SOLD_DATE_STREAM)
            + 1;
    if (soldDate > TODAY) {
      soldDate = -1;
    }
    customer = salesRandom.uniform(SOLD_CUSTOMER_STREAM, 1, scale.customers);
    demographics = salesRandom.uniform(SOLD_DEMOGRAPHICS_STREAM, 1, 1920800);
    household = salesRandom.uniform(SOLD_HOUSEHOLD_STREAM, 1, 7200);
    address = salesRandom.uniform(SOLD_ADDRESS_STREAM, 1, scale.addresses);
    remaining = (int) salesRandom.uniform(TICKET_NUMBER_STREAM, 8, 16);
    // The reference increments the selected one-based position before reading the permutation.
    itemPosition = (int) salesRandom.uniform(SOLD_ITEM_STREAM, 1, items.length) - 1;
  }

  private void priceSale() {
    quantity = salesRandom.uniform(SALES_PRICING, 1, 100);
    wholesale = salesRandom.uniform(SALES_PRICING, 100, 10000);
    listPrice = wholesale * (100 + salesRandom.uniform(SALES_PRICING, 0, 100)) / 100;
    salesPrice = listPrice * (100 - salesRandom.uniform(SALES_PRICING, 0, 100)) / 100;
    long couponRate = salesRandom.uniform(SALES_PRICING, 0, 100);
    coupon =
        salesRandom.uniform(SALES_PRICING, 1, 100) <= 20
            ? salesPrice * quantity * couponRate / 100
            : 0;
    paid = salesPrice * quantity - coupon;
    salesRandom.uniform(
        SALES_PRICING, 0, 50); // The sales stream includes shipping even for store sales.
    taxRate = salesRandom.uniform(SALES_PRICING, 0, 9);
    tax = paid * taxRate / 100;
  }

  private void writeSale(long bitmap) {
    values[0] = soldDate;
    values[1] = soldTime;
    values[2] = item;
    values[3] = customer;
    values[4] = demographics;
    values[5] = household;
    values[6] = address;
    values[7] = store;
    values[8] = promotion;
    values[9] = order;
    values[10] = quantity;
    values[11] = wholesale;
    values[12] = listPrice;
    values[13] = salesPrice;
    // The pinned reference prints coupon_amt in both these positions.
    values[14] = coupon;
    values[15] = salesPrice * quantity;
    values[16] = wholesale * quantity;
    values[17] = listPrice * quantity;
    values[18] = tax;
    values[19] = coupon;
    values[20] = paid;
    values[21] = paid + tax;
    values[22] = paid - wholesale * quantity;
    nulls = 0;
    for (int i = 0; i < values.length; i++) {
      if ((bitmap & (1L << SALES_NULL_BITS[i])) != 0 || (i < 10 && values[i] == -1)) {
        nulls |= 1L << i;
      }
    }
  }

  private void writeReturn() {
    nulls = nullBitmap(returnsRandom, RETURNS_NULLS, 700);
    long returningCustomer = returnsRandom.uniform(RETURN_CUSTOMER_STREAM, 1, scale.customers);
    if (returnsRandom.uniform(RETURN_CUSTOMER_CHOICE_STREAM, 1, 100) < 80) {
      returningCustomer = customer;
    }
    returnsRandom.uniform(
        292, 1998, 2002); // Consumed by the reference date join before choosing lag.
    values[0] = soldDate + returnsRandom.uniform(RETURNED_DATE_STREAM, 4, 180);
    values[1] = returnsRandom.uniform(RETURNED_TIME_STREAM, 28799, 61199);
    values[2] = item;
    values[3] = returningCustomer;
    values[4] = returnsRandom.uniform(RETURN_DEMOGRAPHICS_STREAM, 1, 1920800);
    values[5] = returnsRandom.uniform(RETURN_HOUSEHOLD_STREAM, 1, 7200);
    values[6] = returnsRandom.uniform(RETURN_ADDRESS_STREAM, 1, scale.addresses);
    values[7] =
        historyKey(returnsRandom.uniform(RETURN_STORE_STREAM, 1, scale.storeIds), 1, scale.stores);
    values[8] = returnsRandom.uniform(RETURN_REASON_STREAM, 1, scale.reasons);
    values[9] = order;
    long returnedQuantity = returnsRandom.uniform(RETURNS_PRICING, 1, quantity);
    long returnedPaid = salesPrice * returnedQuantity;
    long shipping =
        listPrice * returnsRandom.uniform(RETURNS_PRICING, 0, 50) / 100 * returnedQuantity;
    long returnedTax = returnedPaid * taxRate / 100;
    long cash = returnedPaid * returnsRandom.uniform(RETURNS_PRICING, 0, 100) / 100;
    long reversed = (returnedPaid - cash) * returnsRandom.uniform(RETURNS_PRICING, 1, 100) / 100;
    long credit = returnedPaid - cash - reversed;
    long fee = returnsRandom.uniform(RETURNS_PRICING, 50, 10000);
    values[10] = returnedQuantity;
    values[11] = returnedPaid;
    values[12] = returnedTax;
    values[13] = returnedPaid + returnedTax;
    values[14] = fee;
    values[15] = shipping;
    values[16] = cash;
    values[17] = reversed;
    values[18] = credit;
    values[19] = shipping + returnedTax + fee;
    for (int i = 0; i < 10; i++) {
      if (values[i] == -1) {
        nulls |= 1L << i;
      }
    }
  }

  private static long nullBitmap(RandomStreams streams, int column, int threshold) {
    long selection = streams.uniform(column, 0, 9999);
    long bitmap = streams.uniform(column, 1, Integer.MAX_VALUE);
    return selection < threshold ? bitmap & ~0x204L : 0;
  }

  private static long historyKey(long id, long date, long maximum) {
    long group = id / 3 * 6;
    int revision = (int) (id % 3);
    long key =
        revision == 1
            ? group + 1
            : revision == 2
                ? group + 2 + (date > HALF_DATE ? 1 : 0)
                : group - 2 + (date > THIRD_DATE ? 1 : 0) + (date > TWO_THIRDS_DATE ? 1 : 0);
    return Math.min(key, maximum);
  }

  private static long julian(int year, int month, int day) {
    return LocalDate.of(year, month, day).toEpochDay() + JULIAN_EPOCH;
  }

  private static int[] calendar(boolean leapYear) {
    int[] weights = new int[366];
    for (int i = 0; i < weights.length; i++) {
      weights[i] = !leapYear && i == 59 ? 0 : i < 213 ? 29 : i < 305 ? 66 : 100;
    }
    return cumulative(weights);
  }

  private static int[] cumulative(int[] weights) {
    int[] result = new int[weights.length];
    int sum = 0;
    for (int i = 0; i < weights.length; i++) {
      sum += weights[i];
      result[i] = sum;
    }
    return result;
  }

  private static int pick(int[] cumulative, RandomStreams random, int column) {
    long value = random.uniform(column, 1, cumulative[cumulative.length - 1]);
    int low = 0;
    int high = cumulative.length - 1;
    while (low < high) {
      int middle = (low + high) >>> 1;
      if (cumulative[middle] >= value) {
        high = middle;
      } else {
        low = middle + 1;
      }
    }
    return low;
  }
}
