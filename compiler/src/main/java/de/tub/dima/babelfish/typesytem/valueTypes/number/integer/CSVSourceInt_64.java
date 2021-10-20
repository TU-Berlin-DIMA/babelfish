package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.storage.UnsafeUtils;
@ExportLibrary(value = IntLibrary.class)
public class CSVSourceInt_64 extends Int_64 {


    private final long startPosition;
    private final long endPosition;

    private long cachedValue;
    public boolean cached;

    public CSVSourceInt_64(long startPosition, long endPosition) {
        this.startPosition = startPosition;
        this.endPosition = endPosition;
    }

    @Override
    public long asLong() {
        long resultValue = 0;
        for (long position = startPosition; position < endPosition; position++) {
            byte value = UnsafeUtils.getByte(position);
            int charValue = value - '0';
            resultValue = resultValue * 10 + charValue;
        }
        cachedValue = resultValue;
        cached = true;
        return resultValue;
    }

    @ExportMessage
    public static class asLongValue{

        @Specialization(guards = "value.cached")
        public static long asLongCached(CSVSourceInt_64 value){
            return value.cachedValue;
        }

        @Specialization()
        public static  long asLong(CSVSourceInt_64 value){
            return value.asLong();
        }

    }
}
