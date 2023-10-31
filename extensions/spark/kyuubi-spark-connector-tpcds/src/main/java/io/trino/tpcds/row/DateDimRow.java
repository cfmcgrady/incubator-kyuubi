package io.trino.tpcds.row;

    import java.util.List;

    import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

    import static com.google.common.collect.Lists.newArrayList;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_CURRENT_DAY;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_CURRENT_MONTH;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_CURRENT_QUARTER;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_CURRENT_WEEK;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_CURRENT_YEAR;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_DATE_ID;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_DATE_SK;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_DAY_NAME;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_DOM;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_DOW;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_FIRST_DOM;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_FOLLOWING_HOLIDAY;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_FY_QUARTER_SEQ;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_FY_WEEK_SEQ;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_FY_YEAR;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_HOLIDAY;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_LAST_DOM;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_MONTH_SEQ;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_MOY;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_QOY;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_QUARTER_NAME;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_QUARTER_SEQ;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_SAME_DAY_LQ;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_SAME_DAY_LY;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_WEEKEND;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_WEEK_SEQ;
    import static io.trino.tpcds.generator.DateDimGeneratorColumn.D_YEAR;
    import static java.lang.String.format;

public class DateDimRow
    extends KyuubiTPCDSTableRowWithNulls
{
  private final long dDateSk;
  private final String dDateId;
  private final int dMonthSeq;
  private final int dWeekSeq;
  private final int dQuarterSeq;
  private final int dYear;
  private final int dDow;
  private final int dMoy;
  private final int dDom;
  private final int dQoy;
  private final int dFyYear;
  private final int dFyQuarterSeq;
  private final int dFyWeekSeq;
  private final String dDayName;
  private final boolean dHoliday;
  private final boolean dWeekend;
  private final boolean dFollowingHoliday;
  private final int dFirstDom;
  private final int dLastDom;
  private final int dSameDayLy;
  private final int dSameDayLq;
  private final boolean dCurrentDay;
  private final boolean dCurrentWeek;
  private final boolean dCurrentMonth;
  private final boolean dCurrentQuarter;
  private final boolean dCurrentYear;

  public DateDimRow(long nullBitMap,
      long dDateSk,
      String dDateId,
      int dMonthSeq,
      int dWeekSeq,
      int dQuarterSeq,
      int dYear,
      int dDow,
      int dMoy,
      int dDom,
      int dQoy,
      int dFyYear,
      int dFyQuarterSeq,
      int dFyWeekSeq,
      String dDayName,
      boolean dHoliday,
      boolean dWeekend,
      boolean dFollowingHoliday,
      int dFirstDom,
      int dLastDom,
      int dSameDayLy,
      int dSameDayLq,
      boolean dCurrentDay,
      boolean dCurrentWeek,
      boolean dCurrentMonth,
      boolean dCurrentQuarter,
      boolean dCurrentYear)
  {
    super(nullBitMap, D_DATE_SK);
    this.dDateSk = dDateSk;
    this.dDateId = dDateId;
    this.dMonthSeq = dMonthSeq;
    this.dWeekSeq = dWeekSeq;
    this.dQuarterSeq = dQuarterSeq;
    this.dYear = dYear;
    this.dDow = dDow;
    this.dMoy = dMoy;
    this.dDom = dDom;
    this.dQoy = dQoy;
    this.dFyYear = dFyYear;
    this.dFyQuarterSeq = dFyQuarterSeq;
    this.dFyWeekSeq = dFyWeekSeq;
    this.dDayName = dDayName;
    this.dHoliday = dHoliday;
    this.dWeekend = dWeekend;
    this.dFollowingHoliday = dFollowingHoliday;
    this.dFirstDom = dFirstDom;
    this.dLastDom = dLastDom;
    this.dSameDayLy = dSameDayLy;
    this.dSameDayLq = dSameDayLq;
    this.dCurrentDay = dCurrentDay;
    this.dCurrentWeek = dCurrentWeek;
    this.dCurrentMonth = dCurrentMonth;
    this.dCurrentQuarter = dCurrentQuarter;
    this.dCurrentYear = dCurrentYear;
  }

  public long getdDateSk() {
    return dDateSk;
  }

  public String getdDateId() {
    return dDateId;
  }

  public int getdMonthSeq() {
    return dMonthSeq;
  }

  public int getdWeekSeq() {
    return dWeekSeq;
  }

  public int getdQuarterSeq() {
    return dQuarterSeq;
  }

  public int getdYear() {
    return dYear;
  }

  public int getdDow() {
    return dDow;
  }

  public int getdMoy() {
    return dMoy;
  }

  public int getdDom() {
    return dDom;
  }

  public int getdQoy() {
    return dQoy;
  }

  public int getdFyYear() {
    return dFyYear;
  }

  public int getdFyQuarterSeq() {
    return dFyQuarterSeq;
  }

  public int getdFyWeekSeq() {
    return dFyWeekSeq;
  }

  public String getdDayName() {
    return dDayName;
  }

  public boolean isdHoliday() {
    return dHoliday;
  }

  public boolean isdWeekend() {
    return dWeekend;
  }

  public boolean isdFollowingHoliday() {
    return dFollowingHoliday;
  }

  public int getdFirstDom() {
    return dFirstDom;
  }

  public int getdLastDom() {
    return dLastDom;
  }

  public int getdSameDayLy() {
    return dSameDayLy;
  }

  public int getdSameDayLq() {
    return dSameDayLq;
  }

  public boolean isdCurrentDay() {
    return dCurrentDay;
  }

  public boolean isdCurrentWeek() {
    return dCurrentWeek;
  }

  public boolean isdCurrentMonth() {
    return dCurrentMonth;
  }

  public boolean isdCurrentQuarter() {
    return dCurrentQuarter;
  }

  public boolean isdCurrentYear() {
    return dCurrentYear;
  }

  @Override public Object[] values() {
    return new Object[] {
        getOrNullForKey(dDateSk, D_DATE_SK),
        getOrNull(dDateId, D_DATE_ID),
        getDateOrNullFromJulianDays(dDateSk, D_DATE_SK),
        getOrNull(dMonthSeq, D_MONTH_SEQ),
        getOrNull(dWeekSeq, D_WEEK_SEQ),
        getOrNull(dQuarterSeq, D_QUARTER_SEQ),
        getOrNull(dYear, D_YEAR),
        getOrNull(dDow, D_DOW),
        getOrNull(dMoy, D_MOY),
        getOrNull(dDom, D_DOM),
        getOrNull(dQoy, D_QOY),
        getOrNull(dFyYear, D_FY_YEAR),
        getOrNull(dFyQuarterSeq, D_FY_QUARTER_SEQ),
        getOrNull(dFyWeekSeq, D_FY_WEEK_SEQ),
        getOrNull(dDayName, D_DAY_NAME),
        getOrNull(format("%4dQ%d", dYear, dQoy), D_QUARTER_NAME),
        getOrNullForBoolean(dHoliday, D_HOLIDAY),
        getOrNullForBoolean(dWeekend, D_WEEKEND),
        getOrNullForBoolean(dFollowingHoliday, D_FOLLOWING_HOLIDAY),
        getOrNull(dFirstDom, D_FIRST_DOM),
        getOrNull(dLastDom, D_LAST_DOM),
        getOrNull(dSameDayLy, D_SAME_DAY_LY),
        getOrNull(dSameDayLq, D_SAME_DAY_LQ),
        getOrNullForBoolean(dCurrentDay, D_CURRENT_DAY),
        getOrNullForBoolean(dCurrentWeek, D_CURRENT_WEEK),
        getOrNullForBoolean(dCurrentMonth, D_CURRENT_MONTH),
        getOrNullForBoolean(dCurrentQuarter, D_CURRENT_QUARTER),
        getOrNullForBoolean(dCurrentYear, D_CURRENT_YEAR)
    };
  }
}
