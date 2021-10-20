package de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat;

public final class Eager_Float_32 extends Float_32 {
    float MAX_VALUE = 0x1.fffffeP+127f;
    float MIN_VALUE = 0x0.000002P-126f;

    private float value;

    public Eager_Float_32(float value) {
        this.value = value;
    }

    @Override
    public float asFloat() {
        return value;
    }

    @Override
    public String toString() {
        return Float.toString(value);
    }

    public void setValue(float value) {
        this.value = value;
    }
}
