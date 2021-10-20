package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import org.apache.arrow.memory.ArrowBuf;

@ExportLibrary(value = IntLibrary.class)
public class ArrowSourceInt_32 extends Int_32 {
    private final ArrowBuf dataBuffer;
    private final long offset;

    public ArrowSourceInt_32(ArrowBuf dataBuffer, long offset) {
        this.dataBuffer = dataBuffer;
        this.offset = offset;
    }

    @Override
    public int asInt() {
        return dataBuffer.getInt(offset);
    }

    @ExportMessage
    public int asIntValue() {
        return dataBuffer.getInt(offset);
    }
}
