package de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat;

public abstract class Float_64 implements BF_Float {
    float MAX_VALUE = 0x1.fffffeP+127f;
    float MIN_VALUE = 0x0.000002P-126f;

    public abstract double asDouble();

    @Override
    public Number defaultMin(){
        return MIN_VALUE;
    }

    @Override
    public Number defaultMax(){
        return MAX_VALUE;
    }

}
