package de.tub.dima.babelfish.benchmark.datatypes;

import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.variableLengthType.MaxLength;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

@LuthRecord(name = "ssb_SUPPLIER")
public class SSB_SUPPLIER implements Record {
    public int s_suppkey;
    @MaxLength(length = 25)
    public Text s_name;
    @MaxLength(length = 25)
    public Text s_address;
    @MaxLength(length = 10)
    public Text s_city;
    @MaxLength(length = 15)
    public Text s_nation;
    @MaxLength(length = 12)
    public Text s_region;
    @MaxLength(length = 15)
    public Text s_phone;

    public SSB_SUPPLIER(int s_suppkey, Text s_name, Text s_address, Text s_city, Text s_nation, Text s_region, Text s_phone) {
        this.s_suppkey = s_suppkey;
        this.s_name = s_name;
        this.s_address = s_address;
        this.s_city = s_city;
        this.s_nation = s_nation;
        this.s_region = s_region;
        this.s_phone = s_phone;
    }
}
