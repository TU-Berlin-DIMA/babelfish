package de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat;

public final class Eager_Float_64 extends Float_64 {
    float MAX_VALUE = 0x1.fffffeP+127f;
    float MIN_VALUE = 0x0.000002P-126f;

    private double value;

    public Eager_Float_64(double value) {
        this.value = value;
    }

    @Override
    public double asDouble() {
        return value;
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }

}
