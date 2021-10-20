package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.storage.UnsafeUtils;

@ExportLibrary(value = IntLibrary.class)
public final class Lazy_Int_8 extends Int_8 {

    private final long address;

    public Lazy_Int_8(long address) {
        this.address = address;
    }

    @Override
    public byte asByte() {
        return UnsafeUtils.getByte(address);
    }

    @ExportMessage
    public byte asByteValue() {
        return UnsafeUtils.getByte(address);
    }

}
