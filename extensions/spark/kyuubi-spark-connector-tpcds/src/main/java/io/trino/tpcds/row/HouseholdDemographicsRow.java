package io.trino.tpcds.row;

    import java.util.List;

    import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

    import static com.google.common.collect.Lists.newArrayList;
    import static io.trino.tpcds.generator.HouseholdDemographicsGeneratorColumn.HD_BUY_POTENTIAL;
    import static io.trino.tpcds.generator.HouseholdDemographicsGeneratorColumn.HD_DEMO_SK;
    import static io.trino.tpcds.generator.HouseholdDemographicsGeneratorColumn.HD_DEP_COUNT;
    import static io.trino.tpcds.generator.HouseholdDemographicsGeneratorColumn.HD_INCOME_BAND_ID;
    import static io.trino.tpcds.generator.HouseholdDemographicsGeneratorColumn.HD_VEHICLE_COUNT;

public class HouseholdDemographicsRow
    extends KyuubiTPCDSTableRowWithNulls
{
  private final long hdDemoSk;
  private final long hdIncomeBandId;
  private final String hdBuyPotential;
  private final int hdDepCount;
  private final int hdVehicleCount;

  public HouseholdDemographicsRow(long nullBitMap, long hdDemoSk, long hdIncomeBandId, String hdBuyPotential, int hdDepCount, int hdVehicleCount)
  {
    super(nullBitMap, HD_DEMO_SK);
    this.hdDemoSk = hdDemoSk;
    this.hdIncomeBandId = hdIncomeBandId;
    this.hdBuyPotential = hdBuyPotential;
    this.hdDepCount = hdDepCount;
    this.hdVehicleCount = hdVehicleCount;
  }

  public long getHdDemoSk() {
    return hdDemoSk;
  }

  public long getHdIncomeBandId() {
    return hdIncomeBandId;
  }

  public String getHdBuyPotential() {
    return hdBuyPotential;
  }

  public int getHdDepCount() {
    return hdDepCount;
  }

  public int getHdVehicleCount() {
    return hdVehicleCount;
  }

  @Override public Object[] values() {
    return new Object[] {
        getOrNullForKey(hdDemoSk, HD_DEMO_SK),
        getOrNullForKey(hdIncomeBandId, HD_INCOME_BAND_ID),
        getOrNull(hdBuyPotential, HD_BUY_POTENTIAL),
        getOrNull(hdDepCount, HD_DEP_COUNT),
        getOrNull(hdVehicleCount, HD_VEHICLE_COUNT)
    };
  }
}
