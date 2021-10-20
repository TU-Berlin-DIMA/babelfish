package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

@ExportLibrary(value = IntLibrary.class)
public final class Eager_Int_8 extends Int_8 {
    final static int MIN_VALUE = -128;
    final static int MAX_VALUE = 127;

    private byte value;

    public Eager_Int_8(byte value) {
        this.value = value;
    }

    public byte asByte() {
        return value;
    }

    @ExportMessage
    public byte asByteValue() {
        return value;
    }


    public Number defaultMin(){
        return MIN_VALUE;
    }


    public Number defaultMax(){
        return MAX_VALUE;
    }

}
