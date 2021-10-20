package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.storage.UnsafeUtils;

@ExportLibrary(value = IntLibrary.class)
public class Lazy_Int_64 extends Int_64 {

    private final long address;

    public Lazy_Int_64(long address) {
        this.address = address;
    }

    public long asLong(){
        return UnsafeUtils.getLong(address);
    }

    @ExportMessage
    public long asLongValue(){
        return UnsafeUtils.getLong(address);
    }
}
