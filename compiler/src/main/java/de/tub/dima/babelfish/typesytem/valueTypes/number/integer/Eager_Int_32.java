package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

@ExportLibrary(value = IntLibrary.class)
public final class Eager_Int_32 extends Int_32 {
    int MIN_VALUE = 0x80000000;
    int MAX_VALUE = 0x7fffffff;

    private int value;

    public Eager_Int_32(int value) {
        this.value = value;
    }

    public int asInt() {
        return value;
    }

    @ExportMessage
    public int asIntValue() {
        return value;
    }

    public void setInt(int value) {
        this.value = value;
    }


    public Number defaultMin() {
        return MIN_VALUE;
    }


    public Number defaultMax() {
        return MAX_VALUE;
    }

}
