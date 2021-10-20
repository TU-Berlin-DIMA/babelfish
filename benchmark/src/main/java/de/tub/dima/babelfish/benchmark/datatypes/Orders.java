package de.tub.dima.babelfish.benchmark.datatypes;

import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.variableLengthType.MaxLength;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

@LuthRecord(name = "Lineitem")
public class Orders implements Record {
    public int o_orderkey;
    public int o_custkey;
    public Char o_orderstatus;
    public float o_totalprice;
    public Date o_orderdate;
    @MaxLength(length = 15)
    public Text o_orderpriority;
    @MaxLength(length = 15)
    public Text o_clerk;
    public int o_shippriority;
    @MaxLength(length = 79)
    public Text o_comment;

    public Orders(int o_orderkey, int o_custkey, Char o_orderstatus, float o_totalprice, Date o_orderdate, Text o_orderpriority, Text o_clerk, int o_shippriority, Text o_comment) {
        this.o_orderkey = o_orderkey;
        this.o_custkey = o_custkey;
        this.o_orderstatus = o_orderstatus;
        this.o_totalprice = o_totalprice;
        this.o_orderdate = o_orderdate;
        this.o_orderpriority = o_orderpriority;
        this.o_clerk = o_clerk;
        this.o_shippriority = o_shippriority;
        this.o_comment = o_comment;
    }
}
