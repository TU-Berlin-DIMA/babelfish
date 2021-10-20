package de.tub.dima.babelfish.ir.pqp.objects.polyglot.numpy;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.*;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.ArrayLibrary;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.BFArray;

/**
 * Wrapper to substitute calls to the numpy np object.
 */
@ExportLibrary(InteropLibrary.class)
public class NumpyWrapper implements TruffleObject {
    private final Object nativeNumpyReference;

    public NumpyWrapper(Object nativeNumpyReference) {
        this.nativeNumpyReference = nativeNumpyReference;
    }

    @ExportMessage
    protected boolean hasMembers() {
        return true;
    }

    @ExportMessage
    protected Object getMembers(boolean includeInternal) {
        return true;
    }

    @ExportMessage
    protected boolean isMemberReadable(String string) {
        return true;
    }

    @ExportMessage
    protected boolean isMemberInvocable(String string) {
        return true;
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
    public Class<? extends TruffleLanguage<?>> getLanguage(){
        return BabelfishEngine.class;
    }

    @ExportMessage(name = "readMember")
    static class ReadNode {

        @Specialization
        static Object readMember(NumpyWrapper receiver, String member) throws UnsupportedMessageException, UnknownIdentifierException {
            throw UnsupportedMessageException.create();
        }

    }

    @ExportMessage(name = "invokeMember")
    static class InvokeNode {

        static boolean isArange(String member) {
            return member.equals("arange");
        }

        static boolean sum(String member) {
            return member.equals("sum");
        }

        static boolean dot(String member) {
            return member.equals("dot");
        }

        static boolean add(String member) {
            return member.equals("add");
        }

        static boolean sub(String member) {
            return member.equals("sub");
        }

        static boolean div(String member) {
            return member.equals("div");
        }

        static boolean sqrt(String member) {
            return member.equals("sqrt");
        }

        static boolean log(String member) {
            return member.equals("log");
        }

        static boolean erf(String member) {
            return member.equals("erf");
        }
        static boolean exp(String member) {
            return member.equals("exp");
        }

        @Specialization(guards = "sum(cachedMember)")
        static Object sum(NumpyWrapper receiver, String member, Object[] arguments,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {

            NumpyArrayWrapper numpyArrayWrapper = (NumpyArrayWrapper) arguments[0];
            BFArray array = numpyArrayWrapper.array;
            return arrayLibrary.sum(array);
        }

        @Specialization(guards = "add(cachedMember)")
        static Object add(NumpyWrapper receiver, String member, Object[] arguments,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {

            NumpyArrayWrapper numpyArrayWrapper = (NumpyArrayWrapper) arguments[0];
            if (arguments[1] instanceof NumpyArrayWrapper) {
                NumpyArrayWrapper numpyArrayRight = (NumpyArrayWrapper) arguments[1];
                BFArray array = arrayLibrary.addArray(numpyArrayWrapper.array, numpyArrayRight.array);
                return new NumpyArrayWrapper(array);
            } else if (arguments[1] instanceof Double) {
                BFArray array = arrayLibrary.addScalar(numpyArrayWrapper.array, (double) arguments[1]);
                return new NumpyArrayWrapper(array);
            }
            return null;
        }

        @Specialization(guards = "sub(cachedMember)")
        static Object sub(NumpyWrapper receiver, String member, Object[] arguments,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {

            NumpyArrayWrapper numpyArrayWrapper = (NumpyArrayWrapper) arguments[1];
            if (arguments[0] instanceof NumpyArrayWrapper) {
                NumpyArrayWrapper numpyArrayRight = (NumpyArrayWrapper) arguments[0];
                BFArray array = arrayLibrary.subArray(numpyArrayWrapper.array, numpyArrayRight.array);
                return new NumpyArrayWrapper(array);
            } else if (arguments[0] instanceof Double) {
                BFArray array = arrayLibrary.subScalarLeft(numpyArrayWrapper.array, (double) arguments[0]);
                return new NumpyArrayWrapper(array);
            }
            return null;
        }

        @Specialization(guards = "div(cachedMember)")
        static Object div(NumpyWrapper receiver, String member, Object[] arguments,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {

            NumpyArrayWrapper numpyArrayWrapper = (NumpyArrayWrapper) arguments[0];
            NumpyArrayWrapper numpyArrayRight = (NumpyArrayWrapper) arguments[1];
            BFArray array = numpyArrayWrapper.array;
            BFArray right = numpyArrayRight.array;
            BFArray res = arrayLibrary.divArray(array, right);
            return new NumpyArrayWrapper(res);
        }

        @Specialization(guards = "dot(cachedMember)")
        static Object dot(NumpyWrapper receiver, String member, Object[] arguments,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {
            NumpyArrayWrapper numpyArrayLeft = (NumpyArrayWrapper) arguments[0];
            if (arguments[1] instanceof NumpyArrayWrapper) {
                NumpyArrayWrapper numpyArrayRight = (NumpyArrayWrapper) arguments[1];
                BFArray array = arrayLibrary.dotArray(numpyArrayLeft.array, numpyArrayRight.array);
                return new NumpyArrayWrapper(array);
            } else if (arguments[1] instanceof Double) {
                BFArray array = arrayLibrary.dotScalar(numpyArrayLeft.array, (double) arguments[1]);
                return new NumpyArrayWrapper(array);
            }
            return null;
        }

        @Specialization(guards = "sqrt(cachedMember)")
        static Object sqrt(NumpyWrapper receiver, String member, Object[] arguments,
                           @Cached(value = "member", allowUncached = true) String cachedMember,
                           @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {
            NumpyArrayWrapper numpyArrayLeft = (NumpyArrayWrapper) arguments[0];
            BFArray array = arrayLibrary.sqrt(numpyArrayLeft.array);
            return new NumpyArrayWrapper(array);
        }

        @Specialization(guards = "log(cachedMember)")
        static Object log(NumpyWrapper receiver, String member, Object[] arguments,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {
            NumpyArrayWrapper numpyArrayLeft = (NumpyArrayWrapper) arguments[0];
            BFArray array = arrayLibrary.log(numpyArrayLeft.array);
            return new NumpyArrayWrapper(array);
        }

        @Specialization(guards = "erf(cachedMember)")
        static Object erf(NumpyWrapper receiver, String member, Object[] arguments,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {
            NumpyArrayWrapper numpyArrayLeft = (NumpyArrayWrapper) arguments[0];
            BFArray array = arrayLibrary.erf(numpyArrayLeft.array);
            return new NumpyArrayWrapper(array);
        }

        @Specialization(guards = "exp(cachedMember)")
        static Object exp(NumpyWrapper receiver, String member, Object[] arguments,
                          @Cached(value = "member", allowUncached = true) String cachedMember,
                          @CachedLibrary(limit = "30") ArrayLibrary arrayLibrary) {
            NumpyArrayWrapper numpyArrayLeft = (NumpyArrayWrapper) arguments[0];
            BFArray array = arrayLibrary.exp(numpyArrayLeft.array);
            return new NumpyArrayWrapper(array);
        }


        @Specialization
        static Object invokeMember(NumpyWrapper receiver, String member, Object[] arguments,
                                   @CachedLibrary(limit = "30") InteropLibrary interop) throws UnsupportedMessageException, UnknownIdentifierException, UnsupportedTypeException, ArityException {
            System.err.println("HCUUCUC");
            return interop.invokeMember(receiver.nativeNumpyReference, member, arguments);
        }

    }
}
