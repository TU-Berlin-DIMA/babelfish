package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

public abstract class Int_8 implements Int {
    final static int MIN_VALUE = -128;
    final static int MAX_VALUE = 127;


    public abstract byte asByte();

    public Number defaultMin(){
        return MIN_VALUE;
    }


    public Number defaultMax(){
        return MAX_VALUE;
    }

    @Override
    public String toString() {
        return String.valueOf(asByte());
    }
}
