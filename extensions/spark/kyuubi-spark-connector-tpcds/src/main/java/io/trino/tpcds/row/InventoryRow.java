package io.trino.tpcds.row;

    import java.util.List;

    import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

    import static com.google.common.collect.Lists.newArrayList;
    import static io.trino.tpcds.generator.InventoryGeneratorColumn.INV_DATE_SK;
    import static io.trino.tpcds.generator.InventoryGeneratorColumn.INV_ITEM_SK;
    import static io.trino.tpcds.generator.InventoryGeneratorColumn.INV_QUANTITY_ON_HAND;
    import static io.trino.tpcds.generator.InventoryGeneratorColumn.INV_WAREHOUSE_SK;

public class InventoryRow
    extends KyuubiTPCDSTableRowWithNulls {
  private final long invDateSk;
  private final long invItemSk;
  private final long invWarehouseSk;
  private final int invQuantityOnHand;

  public InventoryRow(long nullBitMap, long invDateSk, long invItemSk, long invWarehouseSk,
      int invQuantityOnHand) {
    super(nullBitMap, INV_DATE_SK);
    this.invDateSk = invDateSk;
    this.invItemSk = invItemSk;
    this.invWarehouseSk = invWarehouseSk;
    this.invQuantityOnHand = invQuantityOnHand;
  }

  public long getInvDateSk() {
    return invDateSk;
  }

  public long getInvItemSk() {
    return invItemSk;
  }

  public long getInvWarehouseSk() {
    return invWarehouseSk;
  }

  public int getInvQuantityOnHand() {
    return invQuantityOnHand;
  }

  @Override public Object[] values() {
    return new Object[] {
        getOrNullForKey(invDateSk, INV_DATE_SK),
        getOrNullForKey(invItemSk, INV_ITEM_SK),
        getOrNullForKey(invWarehouseSk, INV_WAREHOUSE_SK),
        getOrNull(invQuantityOnHand, INV_QUANTITY_ON_HAND)
    };
  }
}
