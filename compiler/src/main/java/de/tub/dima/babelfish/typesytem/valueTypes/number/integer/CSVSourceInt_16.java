package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.storage.UnsafeUtils;
@ExportLibrary(value = IntLibrary.class)
public class CSVSourceInt_16 extends Int_16 {


    private final long startPosition;
    private final long endPosition;

    private short cachedValue;
    public boolean cached;

    public CSVSourceInt_16(long startPosition, long endPosition) {
        this.startPosition = startPosition;
        this.endPosition = endPosition;
    }

    @Override
    public short asShort() {
        int resultValue = 0;
        for (long position = startPosition; position < endPosition; position++) {
            byte value = UnsafeUtils.getByte(position);
            int charValue = value - '0';
            resultValue = resultValue * 10 + charValue;
        }
        cachedValue = (short) resultValue;
        cached = true;
        return (short) resultValue;
    }

    @ExportMessage
    public static class asShortValue{

        @Specialization(guards = "value.cached")
        public static short asShortCached(CSVSourceInt_16 value){
            return value.cachedValue;
        }

        @Specialization()
        public static short asShort(CSVSourceInt_16 value){
            return value.asShort();
        }

    }
}
