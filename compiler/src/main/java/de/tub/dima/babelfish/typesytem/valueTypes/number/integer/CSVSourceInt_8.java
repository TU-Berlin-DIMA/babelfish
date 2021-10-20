package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.storage.UnsafeUtils;

@ExportLibrary(value = IntLibrary.class)
public class CSVSourceInt_8 extends Int_8 {


    private final long startPosition;
    private final long endPosition;

    private byte cachedValue;
    public boolean cached;

    public CSVSourceInt_8(long startPosition, long endPosition) {
        this.startPosition = startPosition;
        this.endPosition = endPosition;
    }

    @Override
    public byte asByte() {
        int resultValue = 0;
        for (long position = startPosition; position < endPosition; position++) {
            byte value = UnsafeUtils.getByte(position);
            int charValue = value - '0';
            resultValue = resultValue * 10 + charValue;
        }
        cachedValue = (byte) resultValue;
        cached = true;
        return (byte) resultValue;
    }

    @ExportMessage
    public static class asByteValue{

        @Specialization(guards = "value.cached")
        public static byte asByteCached(CSVSourceInt_8 value){
            return value.cachedValue;
        }

        @Specialization()
        public static byte asByte(CSVSourceInt_8 value){
            return value.asByte();
        }

    }
}
