package de.tub.dima.babelfish.benchmark.layout;


import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Precision;

@LuthRecord(name = "CachlineRecord")
public class CachlineRecord implements Record {

    @Precision(0)
    public Numeric field_1;
    @Precision(0)
    public Numeric field_2;
    @Precision(0)
    public Numeric field_3;
    @Precision(0)
    public Numeric field_4;
    @Precision(0)
    public Numeric field_5;
    @Precision(0)
    public Numeric field_6;
    @Precision(0)
    public Numeric field_7;
    @Precision(0)
    public Numeric field_8;
    @Precision(0)
    public Numeric field_9;
    @Precision(0)
    public Numeric field_10;
    @Precision(0)
    public Numeric field_11;
    @Precision(0)
    public Numeric field_12;
    @Precision(0)
    public Numeric field_13;
    @Precision(0)
    public Numeric field_14;
    @Precision(0)
    public Numeric field_15;
    @Precision(0)
    public Numeric field_16;

    public CachlineRecord(Numeric field_1, Numeric field_2, Numeric field_3, Numeric field_4, Numeric field_5, Numeric field_6, Numeric field_7, Numeric field_8, Numeric field_9, Numeric field_10, Numeric field_11, Numeric field_12, Numeric field_13, Numeric field_14, Numeric field_15, Numeric field_16) {
        this.field_1 = field_1;
        this.field_2 = field_2;
        this.field_3 = field_3;
        this.field_4 = field_4;
        this.field_5 = field_5;
        this.field_6 = field_6;
        this.field_7 = field_7;
        this.field_8 = field_8;
        this.field_9 = field_9;
        this.field_10 = field_10;
        this.field_11 = field_11;
        this.field_12 = field_12;
        this.field_13 = field_13;
        this.field_14 = field_14;
        this.field_15 = field_15;
        this.field_16 = field_16;
    }

    public CachlineRecord(Numeric field_1) {
        this.field_1 = field_1;
        this.field_2 = field_1;
        this.field_3 = field_1;
        this.field_4 = field_1;
        this.field_5 = field_1;
        this.field_6 = field_1;
        this.field_7 = field_1;
        this.field_8 = field_1;
        this.field_9 = field_1;
        this.field_10 = field_1;
        this.field_11 = field_1;
        this.field_12 = field_1;
        this.field_13 = field_1;
        this.field_14 = field_1;
        this.field_15 = field_1;
        this.field_16 = field_1;
    }
}
