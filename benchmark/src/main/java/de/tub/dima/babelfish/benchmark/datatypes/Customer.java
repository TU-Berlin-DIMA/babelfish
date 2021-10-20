package de.tub.dima.babelfish.benchmark.datatypes;

import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.variableLengthType.MaxLength;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

@LuthRecord(name = "Lineitem")
public class Customer implements Record {
    public int c_custkey;
    @MaxLength(length = 25)
    public Text c_name;
    @MaxLength(length = 40)
    public Text c_address;
    public int c_nationkey;
    @MaxLength(length = 15)
    public Text c_phone;
    public float c_acctbal;
    @MaxLength(length = 10)
    public Text c_mktsegment;
    @MaxLength(length = 117)
    public Text c_comment;

    public Customer(int c_custkey, Text c_name, Text c_address, int c_nationkey, Text c_phone, float c_acctbal, Text c_mktsegment, Text c_comment) {
        this.c_custkey = c_custkey;
        this.c_name = c_name;
        this.c_address = c_address;
        this.c_nationkey = c_nationkey;
        this.c_phone = c_phone;
        this.c_acctbal = c_acctbal;
        this.c_mktsegment = c_mktsegment;
        this.c_comment = c_comment;
    }
}
