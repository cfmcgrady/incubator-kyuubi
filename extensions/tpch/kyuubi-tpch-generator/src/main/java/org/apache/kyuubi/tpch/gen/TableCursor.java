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

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

/** Primitive, reusable output buffers; child tables advance in complete parent groups. */
final class TableCursor implements RowCursor {
  private static final long FIRST_DAY = LocalDate.of(1992, 1, 1).toEpochDay();
  private static final long CURRENT_DAY = LocalDate.ofYearDay(1995, 168).toEpochDay();
  private static final byte[] ALPHABET =
      ascii("0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,");
  private static final byte[] CUSTOMER_PREFIX = ascii("Customer#");
  private static final byte[] SUPPLIER_PREFIX = ascii("Supplier#");
  private static final byte[] CLERK_PREFIX = ascii("Clerk#");
  private static final byte[] MANUFACTURER_PREFIX = ascii("Manufacturer#");
  private static final byte[] BRAND_PREFIX = ascii("Brand#");
  private static final byte[] CUSTOMER_MARKER = ascii("Customer ");
  private static final byte[] COMPLAINTS = ascii("Complaints");
  private static final byte[] RECOMMENDS = ascii("Recommends");
  private static final byte[] FLAG_N = ascii("N");
  private static final byte[] STATUS_F = ascii("F");
  private static final byte[] STATUS_O = ascii("O");
  private static final byte[] STATUS_P = ascii("P");
  private static final String[] DATES = new String[2557];

  static {
    for (int i = 0; i < DATES.length; i++)
      DATES[i] = LocalDate.ofEpochDay(FIRST_DAY + i).toString();
  }

  private final TpchTable table;
  private final RandomStreams random;
  private final long end, customers, parts, suppliers, clerks;
  private final boolean wide;
  private final long[] numbers;
  private final TextValue[] strings;
  private final long[][] lineNumbers;
  private final TextValue[][] lineStrings;
  private final int[] colors;
  private long position;
  private int child = 0, children = 0;
  private long[] currentNumbers;
  private TextValue[] currentStrings;
  private boolean valid;

  TableCursor(TpchTable table, double scale, RowRange range) {
    long total = table.parentRows(scale);
    if (range.end() > total) throw new IllegalArgumentException("Range exceeds table cardinality");
    this.table = table;
    position = range.start();
    end = range.end();
    customers = TpchTable.CUSTOMER.parentRows(scale);
    parts = TpchTable.PART.parentRows(scale);
    suppliers = TpchTable.SUPPLIER.parentRows(scale);
    clerks = Math.max(1000, (long) scale * 1000);
    wide = scale >= 30000;
    random = new RandomStreams(table, range.start());
    numbers = new long[table.columnCount()];
    strings = textValues(table);
    lineNumbers = table == TpchTable.LINE_ITEM ? new long[7][16] : null;
    lineStrings = table == TpchTable.LINE_ITEM ? new TextValue[7][] : null;
    if (lineStrings != null) {
      for (int i = 0; i < lineStrings.length; i++) lineStrings[i] = textValues(table);
    }
    colors = table == TpchTable.PART ? new int[92] : null;
    currentNumbers = numbers;
    currentStrings = strings;
  }

  private static TextValue[] textValues(TpchTable table) {
    TextValue[] values = new TextValue[table.columnCount()];
    for (int i = 0; i < values.length; i++) {
      if (table.column(i).getKind() == TpchTable.Kind.VARCHAR)
        values[i] = new TextValue(table.column(i).getLength());
    }
    return values;
  }

  private static byte[] ascii(String value) {
    return value.getBytes(StandardCharsets.US_ASCII);
  }

  @Override
  public boolean next() {
    valid = false;
    if (table == TpchTable.LINE_ITEM && child < children) {
      currentNumbers = lineNumbers[child];
      currentStrings = lineStrings[child++];
      valid = true;
      return true;
    }
    if (table == TpchTable.PART_SUPPLIER && child < children) {
      partSupplier(position, child++);
      if (child == children) random.finish();
      valid = true;
      return true;
    }
    if (position >= end) return false;
    position++;
    switch (table) {
      case CUSTOMER:
        customer(position);
        break;
      case SUPPLIER:
        supplier(position);
        break;
      case PART:
        part(position);
        break;
      case PART_SUPPLIER:
        children = 4;
        child = 1;
        partSupplier(position, 0);
        valid = true;
        return true;
      case NATION:
        nation(position);
        break;
      case REGION:
        region(position);
        break;
      case ORDERS:
      case LINE_ITEM:
        order(position);
        if (table == TpchTable.LINE_ITEM) {
          child = 1;
          currentNumbers = lineNumbers[0];
          currentStrings = lineStrings[0];
        }
        break;
      default:
        throw new IllegalStateException("Unsupported table: " + table);
    }
    random.finish();
    valid = true;
    return true;
  }

  private void customer(long key) {
    numbers[0] = key;
    identifier(strings[1], CUSTOMER_PREFIX, key);
    address(strings[2], C_ADDRESS);
    numbers[3] = random.uniform(C_NATION, 0, 24);
    phone(strings[4], numbers[3], C_PHONE);
    numbers[5] = random.uniform(C_BALANCE, -99999, 999999);
    strings[6].set(pick("msegmnt", C_SEGMENT));
    text(strings[7], C_COMMENT, 73);
  }

  private void supplier(long key) {
    numbers[0] = key;
    identifier(strings[1], SUPPLIER_PREFIX, key);
    address(strings[2], S_ADDRESS);
    numbers[3] = random.uniform(S_NATION, 0, 24);
    phone(strings[4], numbers[3], S_PHONE);
    numbers[5] = random.uniform(S_BALANCE, -99999, 999999);
    TextValue comment = strings[6];
    text(comment, S_COMMENT, 63);
    long marked = random.uniform(S_MARKED_COMMENT, 1, 10000),
        type = random.uniform(S_COMMENT_TYPE, 0, 100);
    int noise = (int) random.uniform(S_NOISE, 0, comment.length - 19);
    int offset = (int) random.uniform(S_COMMENT_OFFSET, 0, comment.length - 19 - noise);
    if (marked <= 10) {
      comment.replace(offset, CUSTOMER_MARKER);
      comment.replace(offset + 9 + noise, type < 50 ? COMPLAINTS : RECOMMENDS);
    }
  }

  private void part(long key) {
    numbers[0] = key;
    for (int i = 0; i < colors.length; i++) colors[i] = i;
    TextValue name = strings[1];
    name.reset();
    byte[][] words = Distributions.get("colors").bytes;
    // Only the first five positions are observed; finish() skips the remaining 87 draws.
    for (int i = 0; i < 5; i++) {
      int swap = (int) random.uniform(P_NAME, i, 91), value = colors[swap];
      colors[swap] = colors[i];
      colors[i] = value;
      if (i != 0) name.append((byte) ' ');
      name.append(words[value]);
    }
    long manufacturer = random.uniform(P_MANUFACTURER, 1, 5);
    strings[2].reset();
    strings[2].append(MANUFACTURER_PREFIX);
    strings[2].appendNumber(manufacturer, 1);
    strings[3].reset();
    strings[3].append(BRAND_PREFIX);
    strings[3].appendNumber(manufacturer * 10 + random.uniform(P_BRAND, 1, 5), 2);
    strings[4].set(pick("p_types", P_TYPE));
    numbers[5] = random.uniform(P_SIZE, 1, 50);
    strings[6].set(pick("p_cntr", P_CONTAINER));
    numbers[7] = price(key);
    text(strings[8], P_COMMENT, 14);
  }

  private void partSupplier(long key, int ordinal) {
    numbers[0] = key;
    numbers[1] = supplierKey(key, ordinal);
    numbers[2] = random.uniform(PS_QUANTITY, 1, 9999);
    numbers[3] = random.uniform(PS_COST, 100, 100000);
    text(strings[4], PS_COMMENT, 124);
  }

  private void nation(long key) {
    Distributions nations = Distributions.get("nations");
    numbers[0] = key - 1;
    strings[1].set(nations.bytes[(int) key - 1]);
    numbers[2] = nations.weight((int) key - 1);
    text(strings[3], N_COMMENT, 72);
  }

  private void region(long key) {
    numbers[0] = key - 1;
    strings[1].set(Distributions.get("regions").bytes[(int) key - 1]);
    text(strings[2], R_COMMENT, 72);
  }

  private void order(long key) {
    boolean outputLines = table == TpchTable.LINE_ITEM;
    long orderKey = ((key >>> 3) << 5) + (key & 7);
    long date = FIRST_DAY + random.uniform(O_DATE, 0, 2405);
    if (!outputLines) {
      long customer = random.key(O_CUSTOMER, customers, wide);
      int delta = 1;
      while (customer % 3 == 0) {
        customer = Math.min(customer + delta, customers);
        delta = -delta;
      }
      numbers[0] = orderKey;
      numbers[1] = customer;
      numbers[4] = date;
      strings[5].set(pick("o_oprio", O_PRIORITY));
      identifier(strings[6], CLERK_PREFIX, random.uniform(O_CLERK, 1, clerks));
      numbers[7] = 0;
      text(strings[8], O_COMMENT, 49);
    }
    children = (int) random.uniform(O_LINE_COUNT, 1, 7);
    long total = 0;
    int finished = 0;
    for (int i = 0; i < children; i++) {
      long quantity = random.uniform(L_QUANTITY, 1, 50);
      long discount = random.uniform(L_DISCOUNT, 0, 10);
      long tax = random.uniform(L_TAX, 0, 8);
      long part = random.key(L_PART, parts, wide);
      long extendedPrice = price(part) * quantity;
      long shipDate = date + random.uniform(L_SHIP_DATE, 1, 121);
      boolean shipped = shipDate <= CURRENT_DAY;
      // Each column has an independent stream. Orders need only amounts and ship status;
      // lineitem needs no customer, priority, clerk, or order comment.
      if (outputLines) {
        long[] n = lineNumbers[i];
        TextValue[] s = lineStrings[i];
        n[0] = orderKey;
        n[1] = part;
        n[2] = supplierKey(part, random.uniform(L_SUPPLIER, 0, 3));
        n[3] = i + 1;
        n[4] = quantity * 100;
        n[5] = extendedPrice;
        n[6] = discount;
        n[7] = tax;
        n[10] = shipDate;
        n[11] = date + random.uniform(L_COMMIT_DATE, 30, 90);
        n[12] = shipDate + random.uniform(L_RECEIPT_DATE, 1, 30);
        s[8].set(n[12] <= CURRENT_DAY ? pick("rflag", L_RETURN_FLAG) : FLAG_N);
        s[9].set(shipped ? STATUS_F : STATUS_O);
        s[13].set(pick("instruct", L_INSTRUCTIONS));
        s[14].set(pick("smode", L_MODE));
        text(s[15], L_COMMENT, 27);
      } else {
        total += (extendedPrice * (100 - discount) / 100) * (100 + tax) / 100;
        if (shipped) finished++;
      }
    }
    if (!outputLines) {
      strings[2].set(finished == children ? STATUS_F : finished > 0 ? STATUS_P : STATUS_O);
      numbers[3] = total;
    }
  }

  private static long price(long key) {
    return 90000 + (key / 10) % 20001 + (key % 1000) * 100;
  }

  private long supplierKey(long part, long ordinal) {
    return (part + ordinal * (suppliers / 4 + (part - 1) / suppliers)) % suppliers + 1;
  }

  private byte[] pick(String distribution, int stream) {
    return Distributions.get(distribution).pickBytes(random, stream);
  }

  private void text(TextValue target, int stream, int average) {
    TextPool.comment(random, stream, average, target);
  }

  private static void identifier(TextValue target, byte[] prefix, long key) {
    target.reset();
    target.append(prefix);
    target.appendNumber(key, 9);
  }

  private void address(TextValue target, int stream) {
    int length = (int) random.uniform(stream, 10, 40);
    target.reset();
    long bits = 0;
    for (int i = 0; i < length; i++) {
      if (i % 5 == 0) bits = random.addressBits(stream);
      target.append(ALPHABET[(int) (bits & 63)]);
      bits >>= 6;
    }
  }

  private void phone(TextValue target, long nation, int stream) {
    target.reset();
    target.appendNumber(10 + nation, 2);
    target.append((byte) '-');
    target.appendNumber(random.uniform(stream, 100, 999), 3);
    target.append((byte) '-');
    target.appendNumber(random.uniform(stream, 100, 999), 3);
    target.append((byte) '-');
    target.appendNumber(random.uniform(stream, 1000, 9999), 4);
  }

  @Override
  public int columnCount() {
    return table.columnCount();
  }

  private void check() {
    if (!valid) throw new IllegalStateException("Cursor is not on a row");
  }

  @Override
  public long getLong(int column) {
    check();
    return currentNumbers[column];
  }

  @Override
  public double getDouble(int column) {
    return getLong(column) / 100.0;
  }

  @Override
  public String getString(int column) {
    check();
    switch (table.column(column).getKind()) {
      case VARCHAR:
        TextValue text = currentStrings[column];
        return new String(text.bytes, text.offset, text.length, StandardCharsets.US_ASCII);
      case DATE:
        return DATES[(int) (currentNumbers[column] - FIRST_DAY)];
      case DOUBLE:
        long value = currentNumbers[column];
        if (table == TpchTable.LINE_ITEM && column == 4) return Long.toString(value / 100);
        long absolute = Math.abs(value), cents = absolute % 100;
        return (value < 0 ? "-" : "") + (absolute / 100) + "." + (cents < 10 ? "0" : "") + cents;
      default:
        return Long.toString(currentNumbers[column]);
    }
  }

  private TextValue varchar(int column) {
    check();
    TextValue value = currentStrings[column];
    if (value == null) throw new IllegalArgumentException("Not a VARCHAR column: " + column);
    return value;
  }

  @Override
  public byte[] getBytes(int column) {
    return varchar(column).bytes;
  }

  @Override
  public int getByteOffset(int column) {
    return varchar(column).offset;
  }

  @Override
  public int getByteLength(int column) {
    return varchar(column).length;
  }
}
