package org.apache.kyuubi.spark.connector.tpcds.row;

import io.trino.tpcds.generator.GeneratorColumn;
import io.trino.tpcds.row.TableRowWithNulls;
import java.util.List;

import scala.None$;
import scala.Option;
import scala.Some;

public abstract class KyuubiTPCDSTableRowWithNulls extends TableRowWithNulls {

  protected KyuubiTPCDSTableRowWithNulls(long nullBitMap, GeneratorColumn firstColumn) {
    super(nullBitMap, firstColumn);
  }

  public Option<Long> getDateOrNullFromJulianDays(Long value, GeneratorColumn column) {
    return (isNull(column) || value < 0) ? (Option) None$.MODULE$ : Some.apply(value);
  }

  public Option<Long> getOrNullForKey(Long value, GeneratorColumn column) {
    return (isNull(column) || value == -1) ? (Option) None$.MODULE$ : Some.apply(value);
  }

  public <T> Option<T> getOrNull(T value, GeneratorColumn column) {
    return isNull(column) ? (Option<T>) None$.MODULE$ : Some.apply(value);
  }

  public Option<Boolean> getOrNullForBoolean(boolean value, GeneratorColumn column) {
    return isNull(column) ? (Option) None$.MODULE$ : Some.apply(value);
  }

  public abstract Object[] values();

  @Override
  public List<String> getValues() {
    throw new UnsupportedOperationException();
  }
}
