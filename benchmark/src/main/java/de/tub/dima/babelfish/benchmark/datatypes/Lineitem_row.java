package de.tub.dima.babelfish.benchmark.datatypes;

import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Precision;

@LuthRecord(name = "Lineitem")
public class Lineitem_row implements Record {
    public int l_row_count;
    public int l_orderkey;
    public int l_partkey;
    public int l_suppkey;
    public int l_linenumber;
    @Precision(value = 2)
    public Numeric l_quantity;
    @Precision(value = 2)
    public Numeric l_extendedprice;
    @Precision(value = 2)
    public Numeric l_discount;
    @Precision(value = 2)
    public Numeric l_tax;
    public Char l_returnflag;
    public Char l_linestatus;
    public Date l_shipdate;
    public Date l_commitdate;
    public Date l_reciptdate;
    // @MaxLength(length = 25)
    // public Text shipinstruct;
    // @MaxLength(length = 10)
    // public Text shipmode;
    // @MaxLength(length = 44)
    // public Text comment;


    public Lineitem_row(int l_row_count, int l_orderkey, int l_partkey, int l_suppkey, int l_linenumber, Numeric l_quantity, Numeric l_extendedprice, Numeric l_discount, Numeric l_tax, Char l_returnflag, Char l_linestatus, Date l_shipdate, Date l_commitdate, Date l_reciptdate) {
        this.l_row_count = l_row_count;
        this.l_orderkey = l_orderkey;
        this.l_partkey = l_partkey;
        this.l_suppkey = l_suppkey;
        this.l_linenumber = l_linenumber;
        this.l_quantity = l_quantity;
        this.l_extendedprice = l_extendedprice;
        this.l_discount = l_discount;
        this.l_tax = l_tax;
        this.l_returnflag = l_returnflag;
        this.l_linestatus = l_linestatus;
        this.l_shipdate = l_shipdate;
        this.l_commitdate = l_commitdate;
        this.l_reciptdate = l_reciptdate;
    }
}
