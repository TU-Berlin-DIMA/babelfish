package de.tub.dima.babelfish.typesytem.variableLengthType.Array;

import com.oracle.graal.python.runtime.object.PythonObjectFactory;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.*;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.pqp.objects.polyglot.numpy.NumpyArrayWrapper;
import de.tub.dima.babelfish.storage.UnsafeUtils;

@ExportLibrary(value = InteropLibrary.class, receiverType = BFArray.class)
@ExportLibrary(value = ArrayLibrary.class, receiverType = BFArray.class)
public class ArrayBuiltins {

    public static boolean isInt(BFArray array) {
        return array.getComponentType() == ComponentTypes.Int_32;
    }

    public static boolean isFloat(BFArray array) {
        return array.getComponentType() == ComponentTypes.Float_32;
    }

    public static boolean isLong(BFArray array) {
        return array.getComponentType() == ComponentTypes.Int_64;
    }

    public static boolean isDouble(BFArray array) {
        return array.getComponentType() == ComponentTypes.Float_64;
    }


    @ExportMessage
    public static boolean hasLanguage(BFArray receiver) {
        return true;
    }

    @ExportMessage
    public static String toDisplayString(BFArray receiver, boolean allowSideEffects) {
        return "";
    }

    @ExportMessage
    public static Class<? extends TruffleLanguage<?>> getLanguage(BFArray receiver){
        return BabelfishEngine.class;
    }


    @ExportMessage
    static public boolean hasMembers(BFArray receiver) {
        return true;
    }

    @ExportMessage
    static public Object getMembers(BFArray receiver, boolean includeInternal) throws UnsupportedMessageException {
        throw UnsupportedMessageException.create();
    }

    @ExportMessage
    static public boolean isMemberInvocable(BFArray receiver, String member) {
        return false;
    }

    @ExportMessage
    static public class invokeMember {
        public static PythonObjectFactory getFactory() {
            return PythonObjectFactory.create();
        }

        @Specialization
        public static NumpyArrayWrapper invokeMember(BFArray receiver, String member, Object[] arguments, @Cached(value = "getFactory()", allowUncached = true) PythonObjectFactory factory)
                throws UnsupportedMessageException, ArityException, UnknownIdentifierException, UnsupportedTypeException {
            return new NumpyArrayWrapper(receiver);
        }
    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class Length {
        @Specialization
        public static int length(BFArray array, @Cached(value = "array", allowUncached = true) BFArray cachedArray) {
            return cachedArray.getLength();
        }
    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class DotArray {

        @Specialization
        public static LazyDotBFArray dotArray(BFArray leftArray, BFArray rightArray, @CachedLibrary(value = "leftArray") ArrayLibrary arrayLibrary) {
            int length = arrayLibrary.length(leftArray);
            return new LazyDotBFArray(leftArray, rightArray, length);
        }
    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class Sqrt {

        @Specialization
        public static LazySqrtBFArray sqrt(BFArray leftArray, @CachedLibrary(value = "leftArray") ArrayLibrary arrayLibrary) {
            int length = arrayLibrary.length(leftArray);
            return new LazySqrtBFArray(leftArray, length);
        }
    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class DivArray {

        @Specialization
        public static LazyDivBFArray divArray(BFArray leftArray, BFArray rightArray, @CachedLibrary(value = "leftArray") ArrayLibrary arrayLibrary) {
            int length = arrayLibrary.length(leftArray);
            return new LazyDivBFArray(leftArray, rightArray, length);
        }
    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class SubArray {

        @Specialization
        public static LazySubBFArray subArray(BFArray leftArray, BFArray rightArray, @CachedLibrary(value = "leftArray") ArrayLibrary arrayLibrary) {
            int length = arrayLibrary.length(leftArray);
            return new LazySubBFArray(leftArray, rightArray, length);
        }
    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class Log {

        @Specialization
        public static LazyLogBFArray log(BFArray leftArray, @CachedLibrary(value = "leftArray") ArrayLibrary arrayLibrary) {
            int length = arrayLibrary.length(leftArray);
            return new LazyLogBFArray(leftArray, length);
        }
    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class AddArray {

        @Specialization
        public static LazyAddBFArray dotArray(BFArray leftArray, BFArray rightArray, @CachedLibrary(value = "leftArray") ArrayLibrary arrayLibrary) {
            int length = arrayLibrary.length(leftArray);
            return new LazyAddBFArray(leftArray, rightArray, length);
        }
    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class AddScalar {

        @Specialization
        public static LazyScalarAddBFArray adScalar(BFArray leftArray, double rightArray, @CachedLibrary(value = "leftArray") ArrayLibrary arrayLibrary) {
            int length = arrayLibrary.length(leftArray);
            return new LazyScalarAddBFArray(leftArray, rightArray, length);
        }
    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class SubScalarLeft {

        @Specialization
        public static LazyScalarSubBFArray subScalarLeft( BFArray leftArray, double rightArray, @CachedLibrary(value = "leftArray") ArrayLibrary arrayLibrary) {
            int length = arrayLibrary.length(leftArray);
            return new LazyScalarSubBFArray(rightArray, leftArray,  length);
        }
    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class DotScalar {

        @Specialization
        public static LazyScalarDotBFArray dotScalar(BFArray leftArray,
                                                     double rightArray,
                                                     @CachedLibrary(value = "leftArray") ArrayLibrary arrayLibrary) {
            int length = arrayLibrary.length(leftArray);
            return new LazyScalarDotBFArray(leftArray, rightArray, length);
        }

    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class Erf {

        @Specialization(guards = "isDouble")
        public static LazyErfBFArray doubleErf(BFArray array,
                                               @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                               @CachedLibrary(value = "array") ArrayLibrary arrayLibrary) {
            int length = arrayLibrary.length(array);
            return new LazyErfBFArray(array, length);
        }

    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class Exp {

        @Specialization(guards = "isDouble")
        public static LazyExpBFArray doubleExp(BFArray array,
                                               @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                               @CachedLibrary(value = "array") ArrayLibrary arrayLibrary) {
            int length = arrayLibrary.length(array);
            return new LazyExpBFArray(array, length);
        }

    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class Sum {

        @Specialization(guards = "isInt")
        @ExplodeLoop()
        public static int intSum(BFArray array,
                                 @Cached(value = "isInt(array)", allowUncached = true) boolean isInt,
                                 @CachedLibrary(value = "array") ArrayLibrary arrayLibrary) {
            int sum = 0;
            int length = arrayLibrary.length(array);
            for (int i = 0; i < length; i++) {
                sum = sum + (int) arrayLibrary.read(array, i);
            }
            return sum;
        }

        @Specialization(guards = "isLong")

        public static long longSum(BFArray array,
                                   @Cached(value = "isLong(array)", allowUncached = true) boolean isLong,
                                   @CachedLibrary(value = "array") ArrayLibrary arrayLibrary) {
            long sum = 0;
            int length = arrayLibrary.length(array);
            for (int i = 0; i < length; i++) {
                sum = sum + (long) arrayLibrary.read(array, i);
            }
            return sum;
        }

        @Specialization(guards = "isFloat")

        public static float floatSum(BFArray array,
                                     @Cached(value = "isFloat(array)", allowUncached = true) boolean isFloat,
                                     @CachedLibrary(value = "array") ArrayLibrary arrayLibrary) {
            float sum = 0;
            int length = arrayLibrary.length(array);
            for (int i = 0; i < length; i++) {
                sum = sum + (float) arrayLibrary.read(array, i);
            }
            return sum;
        }

        @Specialization(guards = "isDouble")

        public static double doubleSum(BFArray array,
                                       @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                       @CachedLibrary(value = "array") ArrayLibrary arrayLibrary) {
            double sum = 0;
            int length = arrayLibrary.length(array);
            for (int i = 0; i < length; i++) {
                sum = sum + (double) arrayLibrary.read(array, i);
            }
            return sum;
        }

    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class Read {

        @Specialization
        public static int readInt(PointerIntBFArray array, int index) {
            long offset = index * 4L;
            return UnsafeUtils.getInt(array.getStartAddress() + offset);
        }

        @Specialization
        public static long readLong(PointerLongBFArray array, int index) {
            long offset = index * 8L;
            return UnsafeUtils.getLong(array.getStartAddress() + offset);
        }

        @Specialization
        public static double readDouble(PointerDoubleBFArray array, int index) {
            long offset = index * 8L;
            return UnsafeUtils.getDouble(array.getStartAddress() + offset);
        }

        @Specialization
        public static float readFloat(PointerFloatBFArray array, int index) {
            long offset = index * 4L;
            return UnsafeUtils.getFloat(array.getStartAddress() + offset);
        }

        @Specialization(guards = "isDouble")
        public static double lazyDotDouble(LazyDotBFArray array,
                                           int index,
                                           @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                           @CachedLibrary(limit = "30") ArrayLibrary arrayLib) {
            BFArray leftArray = array.leftSource;
            BFArray rightArray = array.rightSource;
            return (double) arrayLib.read(leftArray, index) * (double) arrayLib.read(rightArray, index);
        }

        @Specialization(guards = "isDouble")
        public static double lazyAddDouble(LazyAddBFArray array,
                                           int index,
                                           @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                           @CachedLibrary(limit = "30") ArrayLibrary arrayLib) {
            BFArray leftArray = array.leftSource;
            BFArray rightArray = array.rightSource;
            return (double) arrayLib.read(leftArray, index) + (double) arrayLib.read(rightArray, index);
        }

        @Specialization(guards = "isDouble")
        public static double lazySubDouble(LazySubBFArray array,
                                           int index,
                                           @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                           @CachedLibrary(limit = "30") ArrayLibrary arrayLib) {
            BFArray leftArray = array.leftSource;
            BFArray rightArray = array.rightSource;
            return (double) arrayLib.read(leftArray, index) - (double) arrayLib.read(rightArray, index);
        }

        @Specialization(guards = "isDouble")
        public static double lazyDivDouble(LazyDivBFArray array,
                                           int index,
                                           @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                           @CachedLibrary(limit = "30") ArrayLibrary arrayLib) {
            BFArray leftArray = array.leftSource;
            BFArray rightArray = array.rightSource;
            return (double) arrayLib.read(leftArray, index) / (double) arrayLib.read(rightArray, index);
        }


        @Specialization(guards = "isDouble")
        public static double lazyScalarDotDouble(LazyScalarDotBFArray array,
                                                 int index,
                                                 @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                                 @Cached(value = "array.rightSource", allowUncached = true) double cached_double,
                                                 @CachedLibrary(limit = "30") ArrayLibrary arrayLib) {
            BFArray leftArray = array.leftSource;
            return (double) arrayLib.read(leftArray, index) * cached_double;
        }

        @Specialization(guards = "isDouble")
        public static double lazyScalarAddDouble(LazyScalarAddBFArray array,
                                                 int index,
                                                 @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                                 @Cached(value = "array.rightSource", allowUncached = true) double cached_double,
                                                 @CachedLibrary(limit = "30") ArrayLibrary arrayLib) {
            BFArray leftArray = array.leftSource;
            return (double) arrayLib.read(leftArray, index) + cached_double;
        }

        @Specialization(guards = "isDouble")
        public static double lazyScalarSubDouble(LazyScalarSubBFArray array,
                                                 int index,
                                                 @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                                 @Cached(value = "array.leftSource", allowUncached = true) double cached_double,
                                                 @CachedLibrary(limit = "30") ArrayLibrary arrayLib) {
            BFArray leftArray = array.rightSource;
            return cached_double - (double) arrayLib.read(leftArray, index);
        }

        @Specialization(guards = "isInt")
        public static int lazyDotInt(LazyDotBFArray array,
                                     int index,
                                     @Cached(value = "isInt(array)", allowUncached = true) boolean isInt,
                                     @CachedLibrary(limit = "30") ArrayLibrary arrayLibLeft,
                                     @CachedLibrary(limit = "30") ArrayLibrary arrayLibRight) {
            BFArray leftArray = array.leftSource;
            BFArray rightArray = array.rightSource;
            return (int) arrayLibLeft.read(leftArray, index) * (int) arrayLibRight.read(rightArray, index);
        }

        @Specialization(guards = "isDouble")
        public static double lazySQRTArrayDouble(LazySqrtBFArray array,
                                                 int index,
                                                 @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                                 @CachedLibrary(limit = "30") ArrayLibrary arrayLib) {
            double value = (double) arrayLib.read(array.leftSource, index);
            return Math.sqrt(value);
        }

        @Specialization(guards = "isDouble")
        public static double lazyExpArrayDouble(LazyExpBFArray array,
                                                 int index,
                                                 @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                                 @CachedLibrary(limit = "30") ArrayLibrary arrayLib) {
            double value = (double) arrayLib.read(array.leftSource, index);
            return Math.exp(value);
        }

        @Specialization(guards = "isDouble")
        public static double lazyLogArrayDouble(LazyLogBFArray array,
                                                int index,
                                                @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                                @CachedLibrary(limit = "30") ArrayLibrary arrayLib) {
            double value = (double) arrayLib.read(array.leftSource, index);
            return Math.log(value);
        }

        @Specialization(guards = "isDouble")
        public static double lazyErfArrayDouble(LazyErfBFArray array,
                                                int index,
                                                @Cached(value = "isDouble(array)", allowUncached = true) boolean isDouble,
                                                @CachedLibrary(limit = "30") ArrayLibrary arrayLib) {
            // fractional error in math formula less than 1.2 * 10 ^ -7.
            // although subject to catastrophic cancellation when z in very close to 0
            // from Chebyshev fitting formula for erf(z) from Numerical Recipes, 6.2
            // code from https://introcs.cs.princeton.edu/java/21function/ErrorFunction.java.html
            double z = (double) arrayLib.read(array.leftSource, index);
            double t = 1.0 / (1.0 + 0.5 * Math.abs(z));

            // use Horner's method
            double ans = 1 - t * Math.exp(-z * z - 1.26551223 +
                    t * (1.00002368 +
                            t * (0.37409196 +
                                    t * (0.09678418 +
                                            t * (-0.18628806 +
                                                    t * (0.27886807 +
                                                            t * (-1.13520398 +
                                                                    t * (1.48851587 + t * (-0.82215223 +
                                                                            t * (0.17087277))))))))));
            if (z >= 0) {
                return ans;
            } else {
                return -ans;
            }
        }

    }

    @ExportMessage(library = ArrayLibrary.class)
    static public class Write {

        @Specialization
        public static void writeInt(PointerIntBFArray array, int index, int value) {
            long offset = index * 4L;
            UnsafeUtils.putInt(array.getStartAddress() + offset, value);
        }

        @Specialization
        public static void writeLong(PointerLongBFArray array, int index, long value) {
            long offset = index * 8L;
            UnsafeUtils.putLong(array.getStartAddress() + offset, value);
        }

        @Specialization
        public static void writeFloat(PointerFloatBFArray array, int index, float value) {
            long offset = index * 4L;
            UnsafeUtils.putFloat(array.getStartAddress() + offset, value);
        }

        @Specialization
        public static void writeDouble(PointerFloatBFArray array, int index, double value) {
            long offset = index * 8L;
            UnsafeUtils.putDouble(array.getStartAddress() + offset, value);
        }
    }
}
