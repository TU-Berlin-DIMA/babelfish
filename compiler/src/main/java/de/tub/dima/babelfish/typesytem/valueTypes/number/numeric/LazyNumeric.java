package de.tub.dima.babelfish.typesytem.valueTypes.number.numeric;

import de.tub.dima.babelfish.storage.UnsafeUtils;


public final class LazyNumeric extends Numeric {

    public long address;
    final int precision;

    public LazyNumeric(long address, int precision) {
        super(precision);
        this.address = address;
        this.precision = precision;
    }

    public long getValue() {
        return UnsafeUtils.getLong(address);
    }

    @Override
    public String toString() {
        return String.valueOf(getValue() / ((double) numericShifts[precision]));
    }

}
