package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.storage.UnsafeUtils;

@ExportLibrary(value = IntLibrary.class)
public final class Lazy_Int_16 extends Int_16 {

    private final long address;

    public Lazy_Int_16(long address) {
        this.address = address;
    }

    @Override
    public short asShort() {
        return UnsafeUtils.getShort(address);
    }
    @ExportMessage
    public short asShortValue() {
        return UnsafeUtils.getShort(address);
    }
}
