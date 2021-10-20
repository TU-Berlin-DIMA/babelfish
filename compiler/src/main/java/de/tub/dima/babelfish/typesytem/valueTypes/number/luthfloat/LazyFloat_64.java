package de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat;

import de.tub.dima.babelfish.storage.UnsafeUtils;

public class LazyFloat_64 extends Float_64 {
    float MAX_VALUE = 0x1.fffffeP+127f;
    float MIN_VALUE = 0x0.000002P-126f;

    private long address;

    private double read() {
        return UnsafeUtils.getDouble(address);
    }

    public LazyFloat_64(long address) {
        this.address = address;
    }


    @Override
    public double asDouble() {
        return read();
    }

    @Override
    public Number defaultMin() {
        return MIN_VALUE;
    }

    @Override
    public Number defaultMax() {
        return MAX_VALUE;
    }

    @Override
    public String toString() {
        return Double.toString(read());
    }

}
