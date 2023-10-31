package io.trino.tpcds.row;

    import io.trino.tpcds.type.Address;
    import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_ADDRESS_CITY;
    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_ADDRESS_COUNTRY;
    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_ADDRESS_COUNTY;
    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_ADDRESS_GMT_OFFSET;
    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_ADDRESS_STATE;
    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_ADDRESS_STREET_NAME1;
    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_ADDRESS_STREET_NUM;
    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_ADDRESS_STREET_TYPE;
    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_ADDRESS_SUITE_NUM;
    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_ADDRESS_ZIP;
    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_WAREHOUSE_ID;
    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_WAREHOUSE_NAME;
    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_WAREHOUSE_SK;
    import static io.trino.tpcds.generator.WarehouseGeneratorColumn.W_WAREHOUSE_SQ_FT;
    import static java.lang.String.format;

public class WarehouseRow
    extends KyuubiTPCDSTableRowWithNulls
{
  private final long wWarehouseSk;
  private final String wWarehouseId;
  private final String wWarehouseName;
  private final int wWarehouseSqFt;
  private final Address wAddress;

  public WarehouseRow(long nullBitMap, long wWarehouseSk, String wWarehouseId, String wWarehouseName, int wWarehouseSqFt, Address wAddress)
  {
    super(nullBitMap, W_WAREHOUSE_SK);
    this.wWarehouseSk = wWarehouseSk;
    this.wWarehouseId = wWarehouseId;
    this.wWarehouseName = wWarehouseName;
    this.wWarehouseSqFt = wWarehouseSqFt;
    this.wAddress = wAddress;
  }

  public long getwWarehouseSk() {
    return wWarehouseSk;
  }

  public String getwWarehouseId() {
    return wWarehouseId;
  }

  public String getwWarehouseName() {
    return wWarehouseName;
  }

  public int getwWarehouseSqFt() {
    return wWarehouseSqFt;
  }

  public Address getwAddress() {
    return wAddress;
  }

  @Override public Object[] values() {
    return new Object[] {
        getOrNullForKey(wWarehouseSk, W_WAREHOUSE_SK),
        getOrNull(wWarehouseId, W_WAREHOUSE_ID),
        getOrNull(wWarehouseName, W_WAREHOUSE_NAME),
        getOrNull(wWarehouseSqFt, W_WAREHOUSE_SQ_FT),
        getOrNull(wAddress.getStreetNumber(), W_ADDRESS_STREET_NUM),
        getOrNull(wAddress.getStreetName(), W_ADDRESS_STREET_NAME1),
        getOrNull(wAddress.getStreetType(), W_ADDRESS_STREET_TYPE),
        getOrNull(wAddress.getSuiteNumber(), W_ADDRESS_SUITE_NUM),
        getOrNull(wAddress.getCity(), W_ADDRESS_CITY),
        getOrNull(wAddress.getCounty(), W_ADDRESS_COUNTY),
        getOrNull(wAddress.getState(), W_ADDRESS_STATE),
        getOrNull(format("%05d", wAddress.getZip()), W_ADDRESS_ZIP),
        getOrNull(wAddress.getCountry(), W_ADDRESS_COUNTRY),
        getOrNull(wAddress.getGmtOffset(), W_ADDRESS_GMT_OFFSET)
    };
  }
}
