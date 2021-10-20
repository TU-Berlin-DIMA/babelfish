package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

@ExportLibrary(value = IntLibrary.class)
public final class Eager_Int_64 extends Int_64 {
    int MIN_VALUE = 0x80000000;
    int MAX_VALUE = 0x7fffffff;

    private long value;

    public Eager_Int_64(long value) {
        this.value = value;
    }

    public long asLong() {
        return value;
    }

    @ExportMessage
    public long asLongValue() {
        return value;
    }


    public Number defaultMin(){
        return MIN_VALUE;
    }


    public Number defaultMax(){
        return MAX_VALUE;
    }

}
