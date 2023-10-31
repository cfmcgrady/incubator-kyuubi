package io.trino.tpcds.row;

import static io.trino.tpcds.generator.CustomerAddressGeneratorColumn.CA_ADDRESS_CITY;
import static io.trino.tpcds.generator.CustomerAddressGeneratorColumn.CA_ADDRESS_COUNTRY;
import static io.trino.tpcds.generator.CustomerAddressGeneratorColumn.CA_ADDRESS_COUNTY;
import static io.trino.tpcds.generator.CustomerAddressGeneratorColumn.CA_ADDRESS_GMT_OFFSET;
import static io.trino.tpcds.generator.CustomerAddressGeneratorColumn.CA_ADDRESS_ID;
import static io.trino.tpcds.generator.CustomerAddressGeneratorColumn.CA_ADDRESS_SK;
import static io.trino.tpcds.generator.CustomerAddressGeneratorColumn.CA_ADDRESS_STATE;
import static io.trino.tpcds.generator.CustomerAddressGeneratorColumn.CA_ADDRESS_STREET_NAME;
import static io.trino.tpcds.generator.CustomerAddressGeneratorColumn.CA_ADDRESS_STREET_NUM;
import static io.trino.tpcds.generator.CustomerAddressGeneratorColumn.CA_ADDRESS_STREET_TYPE;
import static io.trino.tpcds.generator.CustomerAddressGeneratorColumn.CA_ADDRESS_SUITE_NUM;
import static io.trino.tpcds.generator.CustomerAddressGeneratorColumn.CA_ADDRESS_ZIP;
import static io.trino.tpcds.generator.CustomerAddressGeneratorColumn.CA_LOCATION_TYPE;
import static java.lang.String.format;

import io.trino.tpcds.type.Address;
import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNulls;

public class CustomerAddressRow extends KyuubiTPCDSTableRowWithNulls {
  private final long caAddrSk;
  private final String caAddrId;
  private final Address caAddress;
  private final String caLocationType;

  public CustomerAddressRow(
      long nullBitMap, long caAddrSk, String caAddrId, Address caAddress, String caLocationType) {
    super(nullBitMap, CA_ADDRESS_SK);
    this.caAddrSk = caAddrSk;
    this.caAddrId = caAddrId;
    this.caAddress = caAddress;
    this.caLocationType = caLocationType;
  }

  public long getCaAddrSk() {
    return caAddrSk;
  }

  public String getCaAddrId() {
    return caAddrId;
  }

  public Address getCaAddress() {
    return caAddress;
  }

  public String getCaLocationType() {
    return caLocationType;
  }

  @Override
  public Object[] values() {
    return new Object[] {
      getOrNullForKey(caAddrSk, CA_ADDRESS_SK),
      getOrNull(caAddrId, CA_ADDRESS_ID),
      getOrNull(caAddress.getStreetNumber(), CA_ADDRESS_STREET_NUM),
      getOrNull(caAddress.getStreetName(), CA_ADDRESS_STREET_NAME),
      getOrNull(caAddress.getStreetType(), CA_ADDRESS_STREET_TYPE),
      getOrNull(caAddress.getSuiteNumber(), CA_ADDRESS_SUITE_NUM),
      getOrNull(caAddress.getCity(), CA_ADDRESS_CITY),
      getOrNull(caAddress.getCounty(), CA_ADDRESS_COUNTY),
      getOrNull(caAddress.getState(), CA_ADDRESS_STATE),
      getOrNull(format("%05d", caAddress.getZip()), CA_ADDRESS_ZIP),
      getOrNull(caAddress.getCountry(), CA_ADDRESS_COUNTRY),
      getOrNull(caAddress.getGmtOffset(), CA_ADDRESS_GMT_OFFSET),
      getOrNull(caLocationType, CA_LOCATION_TYPE)
    };
  }
}
