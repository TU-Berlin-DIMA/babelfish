package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.storage.UnsafeUtils;
@ExportLibrary(value = IntLibrary.class)
public class CSVSourceInt_32 extends Int_32 {


    private final long startPosition;
    private final long endPosition;

    private int cachedValue;
    public boolean cached;

    public CSVSourceInt_32(long startPosition, long endPosition) {
        this.startPosition = startPosition;
        this.endPosition = endPosition;
    }

    @Override
    public int asInt() {
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
    public static class asIntValue{

        @Specialization(guards = "value.cached")
        public static int asIntCached(CSVSourceInt_32 value){
            return value.cachedValue;
        }

        @Specialization()
        public static int asInt(CSVSourceInt_32 value){
            return value.asInt();
        }

    }
}
