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

/** Direct generation of the 17 dimensions and inventory, without reflection or row objects. */
final class DimensionGenerator extends GeneratedRow {
  private static final String[] DEMOGRAPHICS = {
    "genders", "marital_statuses", "education", "purchase_band", "credit_ratings"
  };
  private static final String[] DAYS = {
    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
  };

  private static final class ItemPrices {
    static final long[] MINIMUM = prices(1);
    static final long[] MAXIMUM = prices(2);

    private static long[] prices(int column) {
      Distribution prices = Distribution.get("item_current_price");
      long[] values = new long[prices.size()];
      for (int i = 0; i < values.length; i++) {
        values[i] =
            new java.math.BigDecimal(prices.value(i, column)).movePointRight(2).longValueExact();
      }
      return values;
    }
  }

  private final Scale scale;
  private final long end;
  private final RandomStreams random;
  private final long[] previousNumbers;
  private final String[] previousStrings;
  private final Address address = new Address();
  private StringBuilder textBuffer;
  private char[] characterBuffer;
  private long index;
  private long changeFlags;
  private boolean firstRevision;

  DimensionGenerator(TpcdsTable table, double scale, RowRange range) {
    super(table);
    this.scale = new Scale(scale);
    if (range.end() > this.scale.rows(table))
      throw new IllegalArgumentException("Range exceeds table cardinality");
    end = range.end();
    long first = range.start() + 1;
    if (table.history && first <= end) {
      long revision = first % 6;
      first -= revision == 0 ? 2 : revision == 3 || revision == 5 ? 1 : 0;
    }
    index = first;
    random = new RandomStreams(table.firstColumn, table.budgets, first - 1);
    previousNumbers = table.history ? new long[columnCount()] : null;
    previousStrings = table.history ? new String[columnCount()] : null;
    while (index <= range.start()) generate();
  }

  public boolean next() {
    positioned = false;
    if (index > end) return false;
    generate();
    positioned = true;
    return true;
  }

  private long uniform(int stream, long min, long max) {
    return random.uniform(stream, min, max);
  }

  private String choose(int stream, String dictionary) {
    return pick(random, stream, dictionary);
  }

  private String text(int stream, int minimum, int maximum) {
    return GeneratorSupport.text(random, stream, minimum, maximum, textBuffer());
  }

  private String word(String dictionary, long key, int maximum) {
    return GeneratorSupport.word(dictionary, key, maximum, textBuffer());
  }

  private StringBuilder textBuffer() {
    if (textBuffer == null) textBuffer = new StringBuilder(320);
    return textBuffer;
  }

  private String characters(int stream, String alphabet, int minimum, int maximum) {
    if (characterBuffer == null) characterBuffer = new char[20];
    return GeneratorSupport.characters(random, stream, alphabet, minimum, maximum, characterBuffer);
  }

  private long key(int stream, TpcdsTable target) {
    return join(random, stream, scale, target, 1);
  }

  private long history(long value, int ordinal) {
    if (!firstRevision && (changeFlags & 1) != 0) value = previousNumbers[ordinal];
    else previousNumbers[ordinal] = value;
    changeFlags >>>= 1;
    return value;
  }

  private String history(String value, int ordinal) {
    if (!firstRevision && (changeFlags & 1) != 0) value = previousStrings[ordinal];
    else previousStrings[ordinal] = value;
    changeFlags >>>= 1;
    return value;
  }

  // Native pointer-valued SCD fields consume a flag but do not retain the old pointer.
  private String pointer(String value) {
    changeFlags >>>= 1;
    return value;
  }

  private void revisions(int stream) {
    long revision = index % 6;
    firstRevision = revision == 1 || revision == 2 || revision == 4;
    long business = index - (revision == 0 ? 2 : revision == 3 || revision == 5 ? 1 : 0);
    long offset = (table == STORE ? 49 : table.ordinal()) * 6L;
    long duration = LAST_DATE - FIRST_DATE;
    long start = FIRST_DATE - offset, finish = -1;
    if (revision == 2) finish = FIRST_DATE + duration / 2 - offset;
    if (revision == 3) start = FIRST_DATE + duration / 2 - offset + 1;
    if (revision == 4) finish = FIRST_DATE + duration / 3 - offset;
    if (revision == 5) {
      start = FIRST_DATE + duration / 3 - offset + 1;
      finish = FIRST_DATE + duration / 3 * 2 - offset;
    }
    if (revision == 0) start = FIRST_DATE + duration / 3 * 2 - offset + 1;
    put(0, index);
    putBusinessKey(1, business);
    put(2, start);
    put(3, finish);
    changeFlags = uniform(stream, 0, Integer.MAX_VALUE - 1);
  }

  private void generate() {
    long bitmap = nullBitmap(random, nullStream(), table);
    switch (table) {
      case CUSTOMER_DEMOGRAPHICS:
        customerDemographics();
        break;
      case HOUSEHOLD_DEMOGRAPHICS:
        householdDemographics();
        break;
      case INCOME_BAND:
        put(0, index);
        put(1, index == 1 ? 0 : (index - 1) * 10000 + 1);
        put(2, index * 10000);
        break;
      case REASON:
        put(0, index);
        putBusinessKey(1, index);
        put(2, Distribution.get("return_reasons").value((int) index - 1, 0));
        break;
      case SHIP_MODE:
        shipMode();
        break;
      case TIME_DIM:
        time();
        break;
      case DATE_DIM:
        dateDimension();
        break;
      case INVENTORY:
        inventory();
        break;
      case CUSTOMER:
        customer();
        break;
      case CUSTOMER_ADDRESS:
        put(0, index);
        putBusinessKey(1, index);
        address.generate(random, CA_ADDRESS, table, scale);
        address.write(this, 2);
        put(12, choose(CA_LOCATION_TYPE, "location_types"));
        break;
      case CATALOG_PAGE:
        catalogPage();
        break;
      case PROMOTION:
        promotion();
        break;
      case WAREHOUSE:
        put(0, index);
        putBusinessKey(1, index);
        put(2, text(W_WAREHOUSE_NAME, 10, 20));
        put(3, uniform(W_WAREHOUSE_SQ_FT, 50000, 1000000));
        address.generate(random, W_WAREHOUSE_ADDRESS, table, scale);
        address.write(this, 4);
        break;
      case ITEM:
        item();
        break;
      case WEB_PAGE:
        webPage();
        break;
      case WEB_SITE:
        webSite();
        break;
      case STORE:
        store();
        break;
      case CALL_CENTER:
        callCenter();
        break;
      default:
        throw new IllegalArgumentException("Not a dimension: " + table);
    }
    applyNulls(bitmap);
    random.finishRow();
    index++;
  }

  private int nullStream() {
    switch (table) {
      case CALL_CENTER:
        return CC_NULLS;
      case CATALOG_PAGE:
        return CP_NULLS;
      case CUSTOMER:
        return C_NULLS;
      case CUSTOMER_ADDRESS:
        return CA_NULLS;
      case CUSTOMER_DEMOGRAPHICS:
        return CD_NULLS;
      case DATE_DIM:
        return D_NULLS;
      case HOUSEHOLD_DEMOGRAPHICS:
        return HD_NULLS;
      case INCOME_BAND:
        return IB_NULLS;
      case INVENTORY:
        return INV_NULLS;
      case ITEM:
        return I_NULLS;
      case PROMOTION:
        return P_NULLS;
      case REASON:
        return R_NULLS;
      case SHIP_MODE:
        return SM_NULLS;
      case STORE:
        return W_STORE_NULLS;
      case TIME_DIM:
        return T_NULLS;
      case WAREHOUSE:
        return W_NULLS;
      case WEB_PAGE:
        return WP_NULLS;
      case WEB_SITE:
        return WEB_NULLS;
      default:
        throw new IllegalArgumentException("Not a dimension");
    }
  }

  private long bitmap(long value, int ordinal, String dictionary) {
    Distribution distribution = Distribution.get(dictionary);
    put(ordinal, distribution.value((int) (value % distribution.size()), 0));
    return value / distribution.size();
  }

  private void customerDemographics() {
    put(0, index);
    long value = index - 1;
    for (int i = 0; i < DEMOGRAPHICS.length; i++) value = bitmap(value, i + 1, DEMOGRAPHICS[i]);
    for (int i = 6; i < 9; i++) {
      put(i, value % 7);
      value /= 7;
    }
  }

  private void householdDemographics() {
    put(0, index);
    put(1, index % 20 + 1);
    long value = bitmap(index / 20, 2, "buy_potential");
    value = bitmap(value, 3, "dep_count");
    bitmap(value, 4, "vehicle_count");
  }

  private void shipMode() {
    put(0, index);
    putBusinessKey(1, index);
    bitmap(bitmap(index, 2, "ship_mode_type"), 3, "ship_mode_code");
    put(4, Distribution.get("ship_mode_carrier").value((int) index - 1, 0));
    put(5, characters(SM_CONTRACT, ALPHANUM, 1, 20));
  }

  private void time() {
    long second = index - 1, hour = second / 3600;
    put(0, second);
    putBusinessKey(1, index);
    put(2, second);
    put(3, hour);
    put(4, second / 60 % 60);
    put(5, second % 60);
    for (int i = 6; i < 10; i++) put(i, Distribution.get("hours").value((int) hour, i - 5));
  }

  private void dateDimension() {
    long julian = julian(1900, 1, 1) + index;
    LocalDate date = date(julian);
    int year = date.getYear(), month = date.getMonthValue(), day = date.getDayOfMonth();
    // The reference treats century years as leap years in calendar lookups (but uses Gregorian
    // Julian conversion).
    int calendarDay =
        date.getDayOfYear() + (year % 100 == 0 && !date.isLeapYear() && month > 2 ? 1 : 0);
    int dow = date.getDayOfWeek().getValue() % 7;
    if (year % 100 == 0 && !date.isLeapYear() && month <= 2) dow = (dow + 6) % 7;
    long week = (index + 6) / 7, quarter = (year - 1900) * 4 + month / 3 + 1;
    Distribution calendar = Distribution.get("calendar");
    long quarterOfYear = calendar.number(calendarDay - 1, 5);
    put(0, julian);
    putBusinessKey(1, julian);
    put(2, julian);
    put(3, (year - 1900) * 12 + month - 1);
    put(4, week);
    put(5, quarter);
    put(6, year);
    put(7, dow);
    put(8, month);
    put(9, day);
    put(10, quarterOfYear);
    put(11, year);
    put(12, quarter);
    put(13, week);
    put(14, DAYS[dow]);
    put(15, year + "Q" + quarterOfYear);
    put(16, calendar.number(calendarDay - 1, 7));
    put(17, dow == 5 || dow == 6 ? 1 : 0);
    put(
        18,
        calendar.number(calendarDay == 1 ? ((year - 1) % 4 == 0 ? 365 : 364) : calendarDay - 2, 7));
    put(19, julian - day + 1);
    put(20, julian - day + calendarDay - day);
    put(21, date.minusYears(1).toEpochDay() + JULIAN_EPOCH);
    LocalDate quarterStart = LocalDate.of(year, (month - 1) / 3 * 3 + 1, 1);
    put(
        22,
        quarterStart.minusMonths(3).toEpochDay()
            + JULIAN_EPOCH
            + julian
            - (quarterStart.toEpochDay() + JULIAN_EPOCH));
    put(23, julian == 8 ? 1 : 0);
    put(24, year == 2003 && week == 2 ? 1 : 0);
    put(25, year == 2003 && month == 1 ? 1 : 0);
    put(26, year == 2003 && quarterOfYear == 1 ? 1 : 0);
    put(27, year == 2003 ? 1 : 0);
  }

  private void inventory() {
    long itemIds = scale.ids(ITEM), value = index - 1;
    long itemId = value % itemIds + 1;
    value /= itemIds;
    long warehouse = value % scale.rows(WAREHOUSE) + 1;
    value /= scale.rows(WAREHOUSE);
    long date = FIRST_DATE + value * 7;
    put(0, date);
    put(1, historyKey(itemId, date, scale.rows(ITEM)));
    put(2, warehouse);
    put(3, uniform(INV_QUANTITY_ON_HAND, 0, 1000));
  }

  private void customer() {
    put(0, index);
    putBusinessKey(1, index);
    put(10, uniform(C_PREFERRED_CUST_FLAG, 1, 100) < 50 ? 1 : 0);
    put(3, key(C_CURRENT_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS));
    put(2, key(C_CURRENT_CDEMO_SK, CUSTOMER_DEMOGRAPHICS));
    put(4, key(C_CURRENT_ADDR_SK, CUSTOMER_ADDRESS));
    Distribution names = Distribution.get("first_names");
    int nameIndex = names.pick(random, C_FIRST_NAME, 2);
    String first = names.value(nameIndex, 0), last = choose(C_LAST_NAME, "last_names");
    put(8, first);
    put(9, last);
    put(7, pick(random, C_SALUTATION, "salutations", names.weight(nameIndex, 1) == 0 ? 1 : 2));
    LocalDate birth = date(uniform(C_BIRTH_DAY, julian(1924, 1, 1), julian(1992, 12, 31)));
    put(11, birth.getDayOfMonth());
    put(12, birth.getMonthValue());
    put(13, birth.getYear());
    String domain = choose(C_EMAIL_ADDRESS, "top_domains");
    int companyLength = (int) uniform(C_EMAIL_ADDRESS, 10, 20);
    String company = characters(C_EMAIL_ADDRESS, ALPHANUM, 1, 20);
    if (company.length() > companyLength) company = company.substring(0, companyLength);
    put(16, first + "." + last + "@" + company + "." + domain);
    put(17, uniform(C_LAST_REVIEW_DATE, TODAY - 365, TODAY));
    long firstSale = uniform(C_FIRST_SALES_DATE_ID, TODAY - 3650, TODAY);
    put(5, firstSale + 30);
    put(6, firstSale);
    put(14, choose(C_BIRTH_COUNTRY, "countries"));
    put(15, "");
  }

  private void catalogPage() {
    long pages = scale.rows(CATALOG_PAGE) / 18 / 6, catalog = (index - 1) / pages;
    long interval = catalog % 18;
    int duration = interval < 2 ? 182 : interval < 6 ? 91 : 30;
    long offset = (interval < 2 ? interval : interval < 6 ? interval - 2 : interval - 6) * duration;
    put(0, index);
    putBusinessKey(1, index);
    put(2, FIRST_DATE + offset + catalog / 18 * 365);
    put(3, numbers[2] + duration - 1);
    put(4, "DEPARTMENT");
    put(5, catalog + 1);
    put(6, (index - 1) % pages + 1);
    put(7, text(CP_DESCRIPTION, 50, 99));
    put(
        8,
        Distribution.get("catalog_page_types").value(interval < 2 ? 0 : interval < 6 ? 1 : 2, 0));
  }

  private void promotion() {
    put(0, index);
    putBusinessKey(1, index);
    put(2, FIRST_DATE + uniform(P_START_DATE_ID, -720, 100));
    put(3, numbers[2] + uniform(P_END_DATE_ID, 1, 60));
    put(4, key(P_ITEM_SK, ITEM));
    put(5, 100000);
    put(6, 1);
    put(7, word("syllables", index, 5));
    put(8, uniform(P_CHANNEL_DMAIL, 0, 511) & 1);
    for (int i = 9; i < 16; i++) put(i, 0);
    put(16, text(P_CHANNEL_DETAILS, 20, 60));
    uniform(P_PURPOSE, 1, 4);
    put(17, "Unknown");
    put(18, 0);
  }

  private void item() {
    Distribution managers = Distribution.get("item_manager_id");
    int manager = managers.pick(random, I_MANAGER_ID, 0);
    put(20, uniform(I_MANAGER_ID, managers.number(manager, 1), managers.number(manager, 2)));
    revisions(I_SCD);
    put(4, history(text(I_ITEM_DESC, 1, 200), 4));
    Distribution prices = Distribution.get("item_current_price");
    int price = prices.pick(random, I_CURRENT_PRICE, 0);
    put(5, uniform(I_CURRENT_PRICE, ItemPrices.MINIMUM[price], ItemPrices.MAXIMUM[price]));
    // SCD_INT in the reference touches decimal flags, not the monetary value.
    changeFlags >>>= 1;
    put(6, history(numbers[5] * uniform(I_WHOLESALE_COST, 30, 90) / 100, 6));
    Distribution categories = Distribution.get("categories");
    int category = categories.pick(random, I_CATEGORY, 0);
    Distribution classes = Distribution.get(categories.value(category, 1));
    int itemClass = classes.pick(random, I_CLASS, 0);
    put(11, category + 1);
    put(12, categories.value(category, 0));
    put(9, history(itemClass + 1, 9));
    put(10, classes.value(itemClass, 0));
    long brand = index % classes.number(itemClass, 1) + 1;
    put(8, word("brand_syllables", (category + 1) * 10 + itemClass + 1, 45) + " #" + brand);
    put(7, history(brand + ((category + 1) * 1000 + itemClass + 1) * 1000L, 7));
    put(15, pointer(pick(random, I_SIZE, "sizes", (int) categories.number(category, 2) + 1)));
    Distribution manufacturers = Distribution.get("item_manufact_id");
    int manufacturer = manufacturers.pick(random, I_MANUFACT_ID, 0);
    put(
        13,
        history(
            uniform(
                I_MANUFACT_ID,
                manufacturers.number(manufacturer, 1),
                manufacturers.number(manufacturer, 2)),
            13));
    put(14, history(word("syllables", numbers[13], 50), 14));
    String formulation = characters(I_FORMULATION, "0123456789", 20, 20),
        color = pick(random, I_FORMULATION, "colors", 1);
    int position = (int) uniform(I_FORMULATION, 0, formulation.length() - color.length() - 1);
    put(
        16,
        history(
            formulation.substring(0, position)
                + color
                + formulation.substring(position + color.length()),
            16));
    put(17, pointer(pick(random, I_COLOR, "colors", 1)));
    put(18, pointer(choose(I_UNITS, "units")));
    put(19, pointer("Unknown"));
    put(21, word("syllables", index, 50));
    key(I_PROMO_SK, PROMOTION);
    uniform(I_PROMO_SK, 1, 100);
  }

  private void webPage() {
    revisions(WP_SCD);
    uniform(WP_CREATION_DATE_SK, 1998, 2002);
    long duration = (julian(2002, 12, 31) - FIRST_DATE) * scale.companies();
    long creation = FIRST_DATE - ((index / 123 + 1) * 17 % duration / 2);
    // Native uses the table parameter (-1) for this test, hence every page draws a creation date.
    creation = uniform(WP_CREATION_DATE_SK, creation, FIRST_DATE);
    put(4, history(creation, 4));
    put(5, history(TODAY - uniform(WP_ACCESS_DATE_SK, 0, 100), 5));
    put(6, history(uniform(WP_AUTOGEN_FLAG, 0, 99) < 30 ? 1 : 0, 6));
    long customer = history(key(WP_CUSTOMER_SK, CUSTOMER), 7);
    put(7, numbers[6] == 0 ? -1 : customer);
    put(8, history("http://www.foo.com", 8));
    put(9, pointer(choose(WP_TYPE, "web_page_use")));
    put(11, history(uniform(WP_LINK_COUNT, 2, 25), 11));
    put(12, history(uniform(WP_IMAGE_COUNT, 1, 7), 12));
    put(13, history(uniform(WP_MAX_AD_COUNT, 0, 4), 13));
    put(
        10,
        history(
            uniform(
                WP_CHAR_COUNT,
                numbers[11] * 125 + numbers[12] * 50,
                numbers[11] * 300 + numbers[12] * 150),
            10));
  }

  private void historicalAddress(int start) {
    pointer(address.city);
    pointer(address.county);
    address.offset = history(address.offset, start + 9);
    pointer(address.state);
    pointer(address.type);
    pointer(address.street);
    pointer("");
    address.number = history(address.number, start);
    address.zip = history(address.zip, start + 7);
    address.write(this, start);
  }

  private void webSite() {
    revisions(WEB_SCD);
    if (firstRevision) {
      uniform(WEB_OPEN_DATE, 1998, 2002);
      uniform(WEB_CLOSE_DATE, 1998, 2002);
      long duration = (julian(2002, 12, 31) - FIRST_DATE) * scale.companies();
      long open = FIRST_DATE - index * 17 % duration / 2;
      put(4, "site_" + index / 6);
      put(5, open);
      long close = open - duration;
      if (index % 4 == 0) close += duration / 2;
      put(6, close > numbers[3] ? -1 : close);
    }
    put(7, "Unknown");
    put(8, history(name(random, WEB_MANAGER), 8));
    put(9, history(uniform(WEB_MARKET_ID, 1, 6), 9));
    put(10, history(text(WEB_MARKET_CLASS, 20, 50), 10));
    put(11, history(text(WEB_MARKET_DESC, 20, 100), 11));
    put(12, history(name(random, WEB_MARKET_MANAGER), 12));
    put(13, history(uniform(WEB_COMPANY_ID, 1, 6), 13));
    put(14, history(word("syllables", numbers[13], 100), 14));
    address.generate(random, WEB_ADDRESS, table, scale);
    historicalAddress(15);
    put(25, history(uniform(WEB_TAX_PERCENTAGE, 0, 12), 25));
  }

  private void store() {
    revisions(W_STORE_SCD);
    long closedPercent = uniform(W_STORE_CLOSED_DATE_ID, 1, 100),
        daysOpen = uniform(W_STORE_CLOSED_DATE_ID, 5, 500);
    put(4, history(closedPercent < 30 ? FIRST_DATE + daysOpen : -1, 4));
    put(5, history(word("syllables", index, 5), 5));
    uniform(W_STORE_TYPE, 1, 1);
    put(6, history(uniform(W_STORE_EMPLOYEES, 200, 300), 6));
    put(7, history(uniform(W_STORE_FLOOR_SPACE, 5000000, 10000000), 7));
    put(8, pointer(choose(W_STORE_HOURS, "call_center_hours")));
    put(9, history(name(random, W_STORE_MANAGER), 9));
    put(10, history(uniform(W_STORE_MARKET_ID, 1, 10), 10));
    put(28, history(uniform(W_STORE_TAX_PERCENTAGE, 0, 11), 28));
    uniform(W_STORE_GEOGRAPHY_CLASS, 1, 4);
    put(11, pointer("Unknown"));
    put(12, history(text(W_STORE_MARKET_DESC, 15, 100), 12));
    put(13, history(name(random, W_STORE_MARKET_MANAGER), 13));
    uniform(W_STORE_DIVISION_NAME, 1, 4);
    put(14, history(1, 14));
    put(15, pointer("Unknown"));
    uniform(W_STORE_COMPANY_NAME, 1, 4);
    put(16, history(1, 16));
    put(17, pointer("Unknown"));
    address.generate(random, W_STORE_ADDRESS, table, scale);
    historicalAddress(18);
  }

  private void callCenter() {
    revisions(CC_SCD);
    if (firstRevision) {
      put(4, -1);
      put(5, FIRST_DATE - 23 - uniform(CC_OPEN_DATE_ID, -365, 0));
      Distribution centers = Distribution.get("call_centers");
      long suffix = index / centers.size();
      put(6, centers.value((int) (index % centers.size()), 0) + (suffix > 0 ? "_" + suffix : ""));
      address.generate(random, CC_ADDRESS, table, scale);
      address.write(this, 20);
    }
    put(7, pointer(choose(CC_CLASS, "call_center_classes")));
    // Native arithmetic for these INTEGER fields wraps at 32 bits at large scales.
    int scaleValue = (int) Math.max(1, scale.value());
    int maximum = 7 * scaleValue * scaleValue;
    put(8, history(uniform(CC_EMPLOYEES, 0, Math.abs((long) maximum) - 1) + 1, 8));
    put(9, history((int) (uniform(CC_SQ_FT, 100, 700) * numbers[8]), 9));
    put(10, pointer(choose(CC_HOURS, "call_center_hours")));
    put(11, history(name(random, CC_MANAGER), 11));
    put(12, history(uniform(CC_MARKET_ID, 1, 6), 12));
    put(13, history(text(CC_MARKET_CLASS, 20, 50), 13));
    put(14, history(text(CC_MARKET_DESC, 20, 100), 14));
    put(15, history(name(random, CC_MARKET_MANAGER), 15));
    put(18, history(uniform(CC_COMPANY, 1, 6), 18));
    put(16, history(uniform(CC_COMPANY, 1, 6), 16));
    put(17, history(word("syllables", numbers[16], 50), 17));
    put(19, history(word("syllables", numbers[18], 10), 19));
    put(30, history(uniform(CC_TAX_PERCENTAGE, 0, 12), 30));
  }
}
