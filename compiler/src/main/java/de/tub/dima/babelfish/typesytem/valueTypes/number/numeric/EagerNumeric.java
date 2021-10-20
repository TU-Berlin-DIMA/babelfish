package de.tub.dima.babelfish.typesytem.valueTypes.number.numeric;


public class EagerNumeric extends Numeric {

    public final long value;

    public EagerNumeric(long value, int precision) {
        super(precision);
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public static int getDivisor(int precision) {
        return numericShifts[precision];
    }


}


