package de.tub.dima.babelfish.benchmark.datatypes;

import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Precision;

@LuthRecord(name = "ssb_LINEORDER")
public class SSB_LINEORDER implements Record {
    public int lo_orderkey;
    public int lo_linenumber;
    public int lo_custkey;
    public int lo_partkey;
    public int lo_suppkey;
    public int lo_orderdate;
    //public Text lo_orderpriority;
    public Char lo_shippriority;
    public int lo_quantity;
    @Precision(value = 2)
    public Numeric lo_extendedprice;
    @Precision(value = 2)
    public Numeric lo_ordtotalprice;
    @Precision(value = 2)
    public Numeric lo_discount;
    @Precision(value = 2)
    public Numeric lo_revenue;
    @Precision(value = 2)
    public Numeric lo_supplycost;
    public int lo_tax;
    //public Text lo_commitdate;


    public SSB_LINEORDER(int lo_orderkey, int lo_linenumber, int lo_custkey, int lo_partkey, int lo_suppkey, int lo_orderdate, Char lo_shippriority, int lo_quantity, Numeric lo_extendedprice, Numeric lo_ordtotalprice, Numeric lo_discount, Numeric lo_revenue, Numeric lo_supplycost, int lo_tax) {
        this.lo_orderkey = lo_orderkey;
        this.lo_linenumber = lo_linenumber;
        this.lo_custkey = lo_custkey;
        this.lo_partkey = lo_partkey;
        this.lo_suppkey = lo_suppkey;
        this.lo_orderdate = lo_orderdate;
        this.lo_shippriority = lo_shippriority;
        this.lo_quantity = lo_quantity;
        this.lo_extendedprice = lo_extendedprice;
        this.lo_ordtotalprice = lo_ordtotalprice;
        this.lo_discount = lo_discount;
        this.lo_revenue = lo_revenue;
        this.lo_supplycost = lo_supplycost;
        this.lo_tax = lo_tax;
    }
}
