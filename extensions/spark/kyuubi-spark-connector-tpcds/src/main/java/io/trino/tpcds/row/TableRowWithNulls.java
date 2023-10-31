package io.trino.tpcds.row;

    import io.trino.tpcds.generator.GeneratorColumn;

    import static io.trino.tpcds.type.Date.fromJulianDays;

public abstract class TableRowWithNulls
    implements TableRow
{
  private long nullBitMap;
  private GeneratorColumn firstColumn;

  protected TableRowWithNulls(long nullBitMap, GeneratorColumn firstColumn)
  {
    this.nullBitMap = nullBitMap;
    this.firstColumn = firstColumn;
  }

  public boolean isNull(GeneratorColumn column)
  {
    long kBitMask = 1L << (column.getGlobalColumnNumber() - firstColumn.getGlobalColumnNumber());
    return (nullBitMap & kBitMask) != 0;
  }

  protected <T> String getStringOrNull(T value, GeneratorColumn column)
  {
    return isNull(column) ? null : value.toString();
  }

  protected <T> String getStringOrNullForKey(long value, GeneratorColumn column)
  {
    return (isNull(column) || value == -1) ? null : Long.toString(value);
  }

  protected <T> String getStringOrNullForBoolean(boolean value, GeneratorColumn column)
  {
    if (isNull(column)) {
      return null;
    }

    return value ? "Y" : "N";
  }

  protected <T> String getDateStringOrNullFromJulianDays(long value, GeneratorColumn column)
  {
    return (isNull(column) || value < 0) ? null : fromJulianDays((int) value).toString();
  }
}
