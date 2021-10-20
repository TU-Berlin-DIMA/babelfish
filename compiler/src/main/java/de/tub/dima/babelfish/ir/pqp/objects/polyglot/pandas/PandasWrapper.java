package de.tub.dima.babelfish.ir.pqp.objects.polyglot.pandas;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.BabelfishEngine;

/**
 * A wrapper for pandas that substitutes common pandas functions
 */
@ExportLibrary(InteropLibrary.class)
public class PandasWrapper implements TruffleObject {
    private final Object nativePandasReference;

    public PandasWrapper(Object nativeNumpyReference) {
        this.nativePandasReference = nativeNumpyReference;
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
    public Class<? extends TruffleLanguage<?>> getLanguage() {
        return BabelfishEngine.class;
    }

    @ExportMessage(name = "readMember")
    static class ReadNode {

        @Specialization
        static Object readMember(PandasWrapper receiver, String member) throws UnsupportedMessageException, UnknownIdentifierException {
            throw UnsupportedMessageException.create();
        }

    }

    @ExportMessage(name = "invokeMember")
    static class InvokeNode {

        static boolean isRead(String member) {
            return member.equals("read");
        }

        static boolean isOr(String member) {
            return member.equals("bor");
        }

        @Specialization(guards = "isRead(cachedMember)")
        static PandasDataframeWrapper read(PandasWrapper receiver, String member, Object[] arguments,
                                           @Cached(value = "member", allowUncached = true) String cachedMember) {

            return new PandasDataframeWrapper.PandasRootDataFrame((String) arguments[0]);
        }

        @Specialization(guards = "isOr(cachedMember)")
        static PandasSeriesWrapper or(PandasWrapper receiver, String member, Object[] arguments,
                                   @Cached(value = "member", allowUncached = true) String cachedMember) {
            PandasSeriesWrapper.ExpressionSeries left = (PandasSeriesWrapper.ExpressionSeries) arguments[0];
            PandasSeriesWrapper.ExpressionSeries right = (PandasSeriesWrapper.ExpressionSeries) arguments[1];
            PandasExpression.BinaryExpression expression = new PandasExpression.BinaryExpression(left.getExpression(), right.getExpression(), PandasExpression.ExpressionType.OR);
            return new PandasSeriesWrapper.ExpressionSeries(expression, null);
        }


    }
}
