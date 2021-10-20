package de.tub.dima.babelfish.ir.pqp.objects.polyglot.numpy;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.ArrayLibrary;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.BFArray;

/**
 * This class provides a wrapper around BFArrays to substitute calls to numpy with efficient primitives similar to Weld.
 * To this end, we define common numpy methods and operators.
 */
@ExportLibrary(InteropLibrary.class)
public class NumpyArrayWrapper implements TruffleObject {

    public final BFArray array;

    public NumpyArrayWrapper(BFArray array) {
        this.array = array;
    }

    @ExportMessage
    protected boolean hasMembers() {
        return true;
    }

    @ExportMessage
    public boolean isBfNode() {
        return true;
    }

    @ExportMessage
    public static class ExecuteBinaryOperation {
        @Specialization(guards = "isDot(cachedMember)")
        public static Object executeBinaryOperation(NumpyArrayWrapper left,
                                                    NumpyArrayWrapper rigt,
                                                    String member,
                                                    @Cached(value = "member", allowUncached = true) String cachedMember,
                                                    @CachedLibrary(value = "left.array") ArrayLibrary arrayLibrary) {
            return new NumpyArrayWrapper(arrayLibrary.dotArray(left.array, rigt.array));
        }

        @Specialization(guards = "isDot(cachedMember)")
        public static Object executeBinaryOperation(NumpyArrayWrapper left,
                                                    Double rigt,
                                                    String member,
                                                    @Cached(value = "member", allowUncached = true) String cachedMember,
                                                    @CachedLibrary(value = "left.array") ArrayLibrary arrayLibrary) {
            return new NumpyArrayWrapper(arrayLibrary.dotScalar(left.array, rigt));
        }

        @Specialization(guards = "add(cachedMember)")
        static Object add(NumpyArrayWrapper left,
                          NumpyArrayWrapper right,
                          String member,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {
            BFArray array = arrayLibrary.addArray(left.array, right.array);
            return new NumpyArrayWrapper(array);
        }

        @Specialization(guards = "add(cachedMember)")
        static Object add(NumpyArrayWrapper left,
                          double right,
                          String member,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {
            BFArray array = arrayLibrary.addScalar(left.array, (double) right);
            return new NumpyArrayWrapper(array);
        }

        @Specialization(guards = "sub(cachedMember)")
        static Object sub(NumpyArrayWrapper left,
                          NumpyArrayWrapper right,
                          String member,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {
            BFArray array = arrayLibrary.subArray(left.array, right.array);
            return new NumpyArrayWrapper(array);
        }

        @Specialization(guards = "sub(cachedMember)")
        static Object sub(NumpyArrayWrapper left,
                          Double right,
                          String member,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {
            BFArray array = arrayLibrary.subScalarLeft(left.array, right);
            return new NumpyArrayWrapper(array);
        }


        @Specialization(guards = "div(cachedMember)")
        static Object div(NumpyArrayWrapper left,
                          NumpyArrayWrapper right,
                          String member,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {
            BFArray array = arrayLibrary.divArray(left.array, right.array);
            return new NumpyArrayWrapper(array);
        }
    }

    static boolean isSum(String member) {
        return member.equals("sum");
    }

    static boolean isDot(String member) {
        return member.equals("dot") || member.equals("*") || member.equals("__mul__");
    }

    static boolean add(String member) {
        return member.equals("add") || member.equals("__add__");
    }

    static boolean sub(String member) {
        return member.equals("sub") || member.equals("__sub__");
    }

    static boolean div(String member) {
        return member.equals("div") || member.equals("__truediv__");
    }


    @ExportMessage
    protected Object getMembers(boolean includeInternal) {
        return true;
    }

    @ExportMessage
    public boolean hasArrayElements() {
        return true;
    }

    @ExportMessage
    public long getArraySize(@CachedLibrary(limit = "30") ArrayLibrary library) {
        return library.length(array);
    }


    @ExportMessage
    public boolean hasLanguage() {
        return true;
    }

    @ExportMessage
    public String toDisplayString(boolean allowSideEffects) {
        return "";
    }

    @ExportMessage
    public Class<? extends TruffleLanguage<?>> getLanguage() {
        return BabelfishEngine.class;
    }


    @ExportMessage
    public Object readArrayElement(long index, @CachedLibrary(limit = "30") ArrayLibrary library) {
        return library.read(array, (int) index);
    }

    @ExportMessage
    public boolean isArrayElementReadable(long index) {
        return true;
    }


    @ExportMessage
    protected boolean isMemberInvocable(String string) {
        return true;
    }

    @ExportMessage(name = "invokeMember")
    static class InvokeNode {


        @Specialization(guards = "isSum(cachedMember)")
        static Object sum(NumpyArrayWrapper receiver, String member, Object[] arguments,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(value = "receiver.array") ArrayLibrary arrayLibrary) {
            return arrayLibrary.sum(receiver.array);
        }

        @Specialization(guards = "isDot(cachedMember)")
        static Object dot(NumpyArrayWrapper receiver, String member, Object[] arguments,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(value = "receiver.array") ArrayLibrary arrayLibrary) {
            NumpyArrayWrapper numpyArrayWrapper = (NumpyArrayWrapper) arguments[0];
            return arrayLibrary.dotArray(receiver.array, numpyArrayWrapper.array);
        }


    }


}
