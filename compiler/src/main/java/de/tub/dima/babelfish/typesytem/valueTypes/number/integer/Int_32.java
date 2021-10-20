package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

public abstract class Int_32 implements Int {
    int MIN_VALUE = 0x80000000;
    int MAX_VALUE = 0x7fffffff;

    public abstract int asInt();

    public Number defaultMin(){
        return MIN_VALUE;
    }

    public Number defaultMax(){
        return MAX_VALUE;
    }
    @Override
    public String toString() {
        return String.valueOf(asInt());
    }
}
