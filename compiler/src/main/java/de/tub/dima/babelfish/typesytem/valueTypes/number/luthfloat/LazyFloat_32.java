package de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat;

import de.tub.dima.babelfish.storage.UnsafeUtils;

public final class LazyFloat_32 extends Float_32 {
    float MAX_VALUE = 0x1.fffffeP+127f;
    float MIN_VALUE = 0x0.000002P-126f;

    private long address;

    public LazyFloat_32(long address) {
        this.address = address;
    }

    @Override
    public float asFloat() {
        return UnsafeUtils.getFloat(address);
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
        return Double.toString(asFloat());
    }

}
