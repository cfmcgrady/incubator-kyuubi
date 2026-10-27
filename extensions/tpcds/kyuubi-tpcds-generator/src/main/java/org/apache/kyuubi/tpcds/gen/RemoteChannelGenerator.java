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

import static org.apache.kyuubi.tpcds.gen.GeneratorSupport.*;
import static org.apache.kyuubi.tpcds.gen.StreamIds.*;
import static org.apache.kyuubi.tpcds.gen.TpcdsTable.*;

import java.time.LocalDate;

/** Catalog and web sales/returns share typed pricing but retain their distinct stream contracts. */
final class RemoteChannelGenerator extends GeneratedRow {
  private final boolean web;
  private final boolean returns;
  private final TpcdsTable salesTable;
  private final TpcdsTable returnsTable;
  private final Scale scale;
  private final RandomStreams salesRandom, returnsRandom;
  private final int[] permutation;
  private final long end;
  private final long[] billing = new long[4], shipping = new long[4];
  private long order,
      soldDate,
      soldTime,
      shipDate,
      item,
      center,
      page,
      site,
      mode,
      warehouse,
      promotion;
  private long catalogDate = FIRST_DATE, catalogBoundary;
  private int remaining, itemPosition;
  private long quantity, wholesale, listPrice, salePrice, coupon, paid, shippingCost, taxRate, tax;

  RemoteChannelGenerator(TpcdsTable table, double scale, RowRange range) {
    super(table);
    web = table == WEB_SALES || table == WEB_RETURNS;
    returns = table == CATALOG_RETURNS || table == WEB_RETURNS;
    salesTable = web ? WEB_SALES : CATALOG_SALES;
    returnsTable = web ? WEB_RETURNS : CATALOG_RETURNS;
    this.scale = new Scale(scale);
    if (range.end() > this.scale.rows(salesTable))
      throw new IllegalArgumentException("Range exceeds order count");
    order = range.start() + 1;
    end = range.end();
    salesRandom = new RandomStreams(salesTable.firstColumn, salesTable.budgets, range.start());
    returnsRandom =
        new RandomStreams(returnsTable.firstColumn, returnsTable.budgets, range.start());
    permutation = new int[range.start() == range.end() ? 0 : (int) this.scale.ids(ITEM)];
    for (int i = 0; i < permutation.length; i++) permutation[i] = i + 1;
    for (int i = 0; i < permutation.length; i++) {
      int other =
          (int) salesRandom.uniform(web ? WS_PERMUTATION : CS_PERMUTE, 0, permutation.length - 1);
      int value = permutation[i];
      permutation[i] = permutation[other];
      permutation[other] = value;
    }
    if (!web && order <= end) {
      catalogBoundary = 1 + dailyOrders(catalogDate);
      if (order > 1) {
        long position = 1;
        while (position < order) {
          position += dailyOrders(catalogDate);
          catalogDate++;
          catalogBoundary = position;
        }
        if (position > order) catalogDate--;
      }
    }
  }

  public boolean next() {
    positioned = false;
    while (order <= end) {
      if (remaining == 0) startOrder();
      long bitmap = nullBitmap(salesRandom, web ? WS_NULLS : CS_NULLS, salesTable);
      shipDate =
          soldDate
              + salesRandom.uniform(
                  web ? WS_SHIP_DATE_SK : CS_SHIP_DATE_SK, web ? 1 : 2, web ? 120 : 90);
      itemPosition = (itemPosition + 1) % permutation.length;
      item = historyKey(permutation[itemPosition], soldDate, scale.rows(ITEM));
      if (web) {
        page = join(salesRandom, WS_WEB_PAGE_SK, scale, WEB_PAGE, soldDate);
        site = join(salesRandom, WS_WEB_SITE_SK, scale, WEB_SITE, soldDate);
      } else page = catalogPage();
      mode = join(salesRandom, web ? WS_SHIP_MODE_SK : CS_SHIP_MODE_SK, scale, SHIP_MODE, 1);
      warehouse = join(salesRandom, web ? WS_WAREHOUSE_SK : CS_WAREHOUSE_SK, scale, WAREHOUSE, 1);
      promotion = join(salesRandom, web ? WS_PROMO_SK : CS_PROMO_SK, scale, PROMOTION, 1);
      priceSale();
      boolean returned = salesRandom.uniform(web ? WR_IS_RETURNED : CR_IS_RETURNED, 0, 99) < 10;
      boolean emit = !returns || returned;
      if (emit) {
        if (returns) writeReturn();
        else writeSale(bitmap);
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

  private long dailyOrders(long date) {
    LocalDate day = date(date);
    Distribution calendar = Distribution.get("calendar");
    int weights = day.isLeapYear() ? 3 : 2;
    long denominator = calendar.total(weights) * 5L;
    return (scale.rows(salesTable) * calendar.weight(day.getDayOfYear() - 1, weights)
            + denominator / 2)
        / denominator;
  }

  private long time(RandomStreams random, int stream) {
    Distribution hours = Distribution.get("hours");
    return hours.number(hours.pick(random, stream, 2), 0) * 3600 + random.uniform(stream, 0, 3599);
  }

  private void person(RandomStreams random, int first, long[] person) {
    person[0] = join(random, first, scale, CUSTOMER, 1);
    person[1] = join(random, first + 1, scale, CUSTOMER_DEMOGRAPHICS, 1);
    person[2] = join(random, first + 2, scale, HOUSEHOLD_DEMOGRAPHICS, 1);
    person[3] = join(random, first + 3, scale, CUSTOMER_ADDRESS, 1);
  }

  private void startOrder() {
    if (web) {
      int year = (int) salesRandom.uniform(WS_SOLD_DATE_SK, 1998, 2002);
      Distribution calendar = Distribution.get("calendar");
      soldDate =
          julian(year, 1, 1)
              + calendar.number(
                  calendar.pick(salesRandom, WS_SOLD_DATE_SK, year == 2000 ? 3 : 2), 0);
      if (soldDate > TODAY) soldDate = -1;
    } else {
      while (order > catalogBoundary) {
        catalogDate++;
        catalogBoundary += dailyOrders(catalogDate);
      }
      soldDate = catalogDate;
    }
    soldTime = time(salesRandom, web ? WS_SOLD_TIME_SK : CS_SOLD_TIME_SK);
    if (!web) center = join(salesRandom, CS_CALL_CENTER_SK, scale, CALL_CENTER, soldDate);
    person(salesRandom, web ? WS_BILL_CUSTOMER_SK : CS_BILL_CUSTOMER_SK, billing);
    long choice = salesRandom.uniform(web ? WS_SHIP_CUSTOMER_SK : CS_SHIP_CUSTOMER_SK, 0, 99);
    if (web ? choice > 7 : choice <= 10)
      person(salesRandom, web ? WS_SHIP_CUSTOMER_SK : CS_SHIP_CUSTOMER_SK, shipping);
    else System.arraycopy(billing, 0, shipping, 0, 4);
    itemPosition =
        (int) salesRandom.uniform(web ? WS_ITEM_SK : CS_SOLD_ITEM_SK, 1, permutation.length) - 1;
    remaining =
        (int)
            salesRandom.uniform(
                web ? WS_ORDER_NUMBER : CS_ORDER_NUMBER, web ? 8 : 4, web ? 16 : 14);
  }

  private long catalogPage() {
    int type = Distribution.get("catalog_page_types").pick(salesRandom, CS_CATALOG_PAGE_SK, 1);
    long page = salesRandom.uniform(CS_CATALOG_PAGE_SK, 1, scale.rows(CATALOG_PAGE) / 18 / 6);
    long offset = soldDate - FIRST_DATE - 1, count = offset / 365 * 18;
    offset %= 365;
    count += type == 0 ? (offset > 183 ? 1 : 0) : type == 1 ? offset / 91 : offset / 31;
    return count * (scale.rows(CATALOG_PAGE) / 18 / 6) + page;
  }

  private void priceSale() {
    int stream = web ? WS_PRICING : CS_PRICING;
    quantity = salesRandom.uniform(stream, 1, 100);
    wholesale = salesRandom.uniform(stream, 100, 10000);
    listPrice = wholesale * (100 + salesRandom.uniform(stream, 0, 200)) / 100;
    salePrice = listPrice * (100 - salesRandom.uniform(stream, 0, 100)) / 100;
    long couponRate = salesRandom.uniform(stream, 0, 100);
    coupon =
        salesRandom.uniform(stream, 1, 100) <= 20 ? salePrice * quantity * couponRate / 100 : 0;
    paid = salePrice * quantity - coupon;
    shippingCost = listPrice * salesRandom.uniform(stream, 0, 50) / 100 * quantity;
    taxRate = salesRandom.uniform(stream, 0, 9);
    tax = paid * taxRate / 100;
  }

  private void writeSale(long bitmap) {
    put(0, soldDate);
    put(1, soldTime);
    put(2, shipDate);
    if (web) put(3, item);
    int offset = web ? 4 : 3;
    for (int i = 0; i < 4; i++) {
      put(offset + i, billing[i]);
      put(offset + 4 + i, shipping[i]);
    }
    if (web) {
      put(12, page);
      put(13, site);
      put(14, mode);
      put(15, warehouse);
    } else {
      put(11, center);
      put(12, page);
      put(13, mode);
      put(14, warehouse);
      put(15, item);
    }
    put(16, promotion);
    put(17, order);
    put(18, quantity);
    put(19, wholesale);
    put(20, listPrice);
    put(21, salePrice);
    put(22, (listPrice - salePrice) * quantity);
    put(23, salePrice * quantity);
    put(24, wholesale * quantity);
    put(25, listPrice * quantity);
    put(26, tax);
    put(27, coupon);
    put(28, shippingCost);
    put(29, paid);
    put(30, paid + tax);
    put(31, paid + shippingCost);
    put(32, paid + shippingCost + tax);
    put(33, paid - wholesale * quantity);
    applyNulls(bitmap);
  }

  private void writeReturn() {
    long bitmap = nullBitmap(returnsRandom, web ? WR_NULLS : CR_NULLS, returnsTable);
    put(2, item);
    if (web) {
      returnDateTime();
      person(returnsRandom, WR_REFUNDED_CUSTOMER_SK, numbersForReturn);
      if (returnsRandom.uniform(WR_RETURNING_CUSTOMER_SK, 0, 99) < 7)
        System.arraycopy(shipping, 0, numbersForReturn, 0, 4);
      for (int i = 0; i < 4; i++) {
        put(3 + i, numbersForReturn[i]);
        put(7 + i, numbersForReturn[i]);
      }
      put(11, page);
      put(12, join(returnsRandom, WR_REASON_SK, scale, REASON, 1));
      put(13, order);
    } else {
      for (int i = 0; i < 4; i++) put(3 + i, billing[i]);
      person(returnsRandom, CR_RETURNING_CUSTOMER_SK, numbersForReturn);
      if (returnsRandom.uniform(CR_RETURNING_CUSTOMER_SK, 0, 99) < 10) {
        numbersForReturn[0] = shipping[0];
        numbersForReturn[1] = shipping[1];
        numbersForReturn[3] = shipping[3];
      }
      for (int i = 0; i < 4; i++) put(7 + i, numbersForReturn[i]);
      returnDateTime();
      put(11, center);
      put(12, page);
      put(13, join(returnsRandom, CR_SHIP_MODE_SK, scale, SHIP_MODE, 1));
      put(14, join(returnsRandom, CR_WAREHOUSE_SK, scale, WAREHOUSE, 1));
      put(15, join(returnsRandom, CR_REASON_SK, scale, REASON, 1));
      put(16, order);
    }
    int stream = web ? WR_PRICING : CR_PRICING, offset = web ? 14 : 17;
    long returnedQuantity = returnsRandom.uniform(stream, 1, quantity),
        returnedPaid = salePrice * returnedQuantity;
    long
        returnedShipping =
            listPrice * returnsRandom.uniform(stream, 0, 50) / 100 * returnedQuantity,
        returnedTax = returnedPaid * taxRate / 100;
    long cash = returnedPaid * returnsRandom.uniform(stream, 0, 100) / 100;
    long reversed = (returnedPaid - cash) * returnsRandom.uniform(stream, 1, 100) / 100,
        credit = returnedPaid - cash - reversed;
    long fee = returnsRandom.uniform(stream, 50, 10000);
    put(offset, returnedQuantity);
    put(offset + 1, returnedPaid);
    put(offset + 2, returnedTax);
    put(offset + 3, returnedPaid + returnedTax);
    put(offset + 4, fee);
    put(offset + 5, returnedShipping);
    put(offset + 6, cash);
    put(offset + 7, reversed);
    put(offset + 8, credit);
    put(offset + 9, returnedShipping + returnedTax + fee);
    applyNulls(bitmap);
  }

  private final long[] numbersForReturn = new long[4];

  private void returnDateTime() {
    int dateStream = web ? WR_RETURNED_DATE_SK : CR_RETURNED_DATE_SK;
    returnsRandom.uniform(dateStream, 1998, 2002);
    put(0, shipDate + returnsRandom.uniform(dateStream, web ? 2 : 4, web ? 240 : 180));
    put(1, time(returnsRandom, web ? WR_RETURNED_TIME_SK : CR_RETURNED_TIME_SK));
  }
}
