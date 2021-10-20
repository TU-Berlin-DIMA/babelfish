package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

@ExportLibrary(value = IntLibrary.class)
public final class Eager_Int_16 extends Int_16 {
    final static int MIN_VALUE = -128;
    final static int MAX_VALUE = 127;

    private short value;

    public Eager_Int_16(short value) {
        this.value = value;
    }


    public short asShort() {
        return value;
    }

    @ExportMessage
    public short asShortValue() {
        return value;
    }


    public Number defaultMin(){
        return MIN_VALUE;
    }


    public Number defaultMax(){
        return MAX_VALUE;
    }

}
