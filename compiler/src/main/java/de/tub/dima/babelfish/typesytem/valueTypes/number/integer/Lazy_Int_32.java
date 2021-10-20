package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.storage.UnsafeUtils;

@ExportLibrary(value = IntLibrary.class)
public final class Lazy_Int_32 extends Int_32 {

    private final long address;

    public Lazy_Int_32(long address) {
        this.address = address;
    }

    @Override
    public int asInt() {
        return UnsafeUtils.getInt(address);
    }

    @ExportMessage
    public int asIntValue() {
        return UnsafeUtils.getInt(address);
    }

}
