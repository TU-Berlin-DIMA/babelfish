package de.tub.dima.babelfish.typesytem.valueTypes.number.numeric;

import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.*;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;


@ExportLibrary(value = NumericLibrary.class, receiverType = Numeric.class)
@ExportLibrary(value = InteropLibrary.class, receiverType = Numeric.class)
public class NumericBuiltins {

    @ExportMessage(library = NumericLibrary.class)
    public static class getValue {

        @Specialization()
        public static long getValue(ArrowSourceNumeric value) {
            return value.getValue();
        }

        @Specialization()
        public static long getValue(EagerNumeric value) {
            return value.getValue();
        }

        @Specialization()
        public static long getValue(LazyNumeric value) {
            return value.getValue();
        }

        @Specialization(guards = "cached")
        public static long getValue(CSVSourceNumeric value, @Cached("value.cached") boolean cached) {
            return value.getCachedValue();
        }

        @Specialization()
        public static long getValue(CSVSourceNumeric value) {
            return value.getValue();
        }
    }

    @ExportMessage
    public static boolean isNumber(Numeric value) {
        return true;
    }

    @ExportMessage
    public static boolean fitsInFloat(Numeric value) {
        return true;
    }


    @ExportMessage
    public static boolean fitsInDouble(Numeric value) {
        return true;
    }

    @ExportMessage
    public static boolean fitsInByte(Numeric value) {
        return false;
    }

    @ExportMessage
    public static boolean fitsInShort(Numeric value) {
        return false;
    }

    @ExportMessage
    public static boolean fitsInInt(Numeric value) {
        return false;
    }

    @ExportMessage
    public static boolean fitsInLong(Numeric value) {
        return false;
    }

    @ExportMessage
    public static class AsFloat {

        public static int getDivisor(int precision) {
            return Numeric.numericShifts[precision];
        }

        @Specialization(guards = "value.precision == cached_precision")
        public static float asFloatCached(Numeric value, @CachedLibrary("value") NumericLibrary numericLib,
                                          @Cached(value = "value.precision", allowUncached = true) int cached_precision,
                                          @Cached(value = "getDivisor(cached_precision)", allowUncached = true) float divisor) {
            long intValue = numericLib.getValue(value);
            return Numeric.getFloatValue(intValue, divisor);
        }

        @Specialization
        public static float asFloat(Numeric value) {
            float divisor = Numeric.numericShifts[value.precision];
            return Numeric.getFloatValue(value.getValue(), divisor);
        }
    }

    @ExportMessage
    public static class AsDouble {
        public static int getDivisor(int precision) {
            return Numeric.numericShifts[precision];
        }

        @Specialization(guards = "value.precision == cached_precision")
        public static double asDoubleCached(Numeric value,
                                            @CachedLibrary("value") NumericLibrary numericLib,
                                            @Cached(value = "value.precision", allowUncached = true) int cached_precision,
                                            @Cached(value = "getDivisor(value.precision)", allowUncached = true) double divisor) {
            long intValue = numericLib.getValue(value);
            return Numeric.getDoubleValue(intValue, divisor);
        }

        @Specialization
        public static double asDouble(Numeric value2) {
            float divisor = Numeric.numericShifts[value2.precision];
            return Numeric.getDoubleValue(value2.getValue(), divisor);
        }
    }

    @ExportMessage
    public static byte asByte(Numeric value) throws UnsupportedMessageException {
        throw UnsupportedMessageException.create();
    }

    @ExportMessage
    public static short asShort(Numeric value) throws UnsupportedMessageException {
        throw UnsupportedMessageException.create();
    }

    @ExportMessage
    public static int asInt(Numeric value) throws UnsupportedMessageException {
        throw UnsupportedMessageException.create();
    }

    @ExportMessage
    public static long asLong(Numeric value) throws UnsupportedMessageException {
        throw UnsupportedMessageException.create();
    }

    @ExportMessage
    protected static boolean hasMembers(Numeric value) {
        return true;
    }

    @ExportMessage
    protected static Object getMembers(Numeric value, boolean includeInternal) {
        return true;
    }


    @ExportMessage
    public static boolean isMemberInvocable(Numeric value, String string) {
        return true;
    }

    @ExportMessage(name = "invokeMember")
    static class InvokeNode {

        static boolean isItem(String member) {
            return member.equals("item");
        }

        @Specialization(guards = "isItem(cachedMember)")
        static Numeric sum(Numeric receiver, String member, Object[] arguments,  @Cached(value = "member", allowUncached = true) String cachedMember) {
            return receiver;
        }
    }
}
