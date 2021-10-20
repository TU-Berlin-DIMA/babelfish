package de.tub.dima.babelfish.benchmark.datatypes;

import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.variableLengthType.MaxLength;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

@LuthRecord(name = "ssb_SUPPLIER")
public class SSB_CUSTOMER implements Record {
    public int c_custkey;
    @MaxLength(length = 25)
    public Text c_name;
    @MaxLength(length = 25)
    public Text c_address;
    @MaxLength(length = 10)
    public Text c_city;
    @MaxLength(length = 15)
    public Text c_nation;
    @MaxLength(length = 12)
    public Text c_region;
    @MaxLength(length = 15)
    public Text c_phone;
    @MaxLength(length = 10)
    public Text c_mktsegment;

    public SSB_CUSTOMER(int c_custkey, Text c_name, Text c_address, Text c_city, Text c_nation, Text c_region, Text c_phone, Text c_mktsegment) {
        this.c_custkey = c_custkey;
        this.c_name = c_name;
        this.c_address = c_address;
        this.c_city = c_city;
        this.c_nation = c_nation;
        this.c_region = c_region;
        this.c_phone = c_phone;
        this.c_mktsegment = c_mktsegment;
    }
}
