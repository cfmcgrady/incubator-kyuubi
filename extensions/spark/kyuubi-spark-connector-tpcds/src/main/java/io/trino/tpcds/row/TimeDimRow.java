package io.trino.tpcds.row;

import static io.trino.tpcds.generator.TimeDimGeneratorColumn.T_AM_PM;
import static io.trino.tpcds.generator.TimeDimGeneratorColumn.T_HOUR;
import static io.trino.tpcds.generator.TimeDimGeneratorColumn.T_MEAL_TIME;
import static io.trino.tpcds.generator.TimeDimGeneratorColumn.T_MINUTE;
import static io.trino.tpcds.generator.TimeDimGeneratorColumn.T_SECOND;
import static io.trino.tpcds.generator.TimeDimGeneratorColumn.T_SHIFT;
import static io.trino.tpcds.generator.TimeDimGeneratorColumn.T_SUB_SHIFT;
import static io.trino.tpcds.generator.TimeDimGeneratorColumn.T_TIME;
import static io.trino.tpcds.generator.TimeDimGeneratorColumn.T_TIME_ID;
import static io.trino.tpcds.generator.TimeDimGeneratorColumn.T_TIME_SK;

import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

public class TimeDimRow extends KyuubiTPCDSTableRowWithNulls {
  private final long tTimeSk;
  private final String tTimeId;
  private final int tTime;
  private final int tHour;
  private final int tMinute;
  private final int tSecond;
  private final String tAmPm;
  private final String tShift;
  private final String tSubShift;
  private final String tMealTime;

  public TimeDimRow(
      long nullBitMap,
      long tTimeSk,
      String tTimeId,
      int tTime,
      int tHour,
      int tMinute,
      int tSecond,
      String tAmPm,
      String tShift,
      String tSubShift,
      String tMealTime) {
    super(nullBitMap, T_TIME_SK);
    this.tTimeSk = tTimeSk;
    this.tTimeId = tTimeId;
    this.tTime = tTime;
    this.tHour = tHour;
    this.tMinute = tMinute;
    this.tSecond = tSecond;
    this.tAmPm = tAmPm;
    this.tShift = tShift;
    this.tSubShift = tSubShift;
    this.tMealTime = tMealTime;
  }

  public long gettTimeSk() {
    return tTimeSk;
  }

  public String gettTimeId() {
    return tTimeId;
  }

  public int gettTime() {
    return tTime;
  }

  public int gettHour() {
    return tHour;
  }

  public int gettMinute() {
    return tMinute;
  }

  public int gettSecond() {
    return tSecond;
  }

  public String gettAmPm() {
    return tAmPm;
  }

  public String gettShift() {
    return tShift;
  }

  public String gettSubShift() {
    return tSubShift;
  }

  public String gettMealTime() {
    return tMealTime;
  }

  @Override
  public Object[] values() {
    return new Object[] {
      getOrNullForKey(tTimeSk, T_TIME_SK),
      getOrNull(tTimeId, T_TIME_ID),
      getOrNull(tTime, T_TIME),
      getOrNull(tHour, T_HOUR),
      getOrNull(tMinute, T_MINUTE),
      getOrNull(tSecond, T_SECOND),
      getOrNull(tAmPm, T_AM_PM),
      getOrNull(tShift, T_SHIFT),
      getOrNull(tSubShift, T_SUB_SHIFT),
      getOrNull(tMealTime, T_MEAL_TIME)
    };
  }
}
