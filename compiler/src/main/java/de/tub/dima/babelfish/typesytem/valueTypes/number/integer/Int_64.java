package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

public abstract class Int_64 implements Int {
    long MIN_VALUE = 0x8000000000000000L;
    long MAX_VALUE = 0x7fffffffffffffffL;

    public abstract long asLong();

    public Number defaultMin(){
        return MIN_VALUE;
    }


    public Number defaultMax(){
        return MAX_VALUE;
    }
    @Override
    public String toString() {
        return String.valueOf(asLong());
    }
}
