package de.tub.dima.babelfish.ir.pqp.objects.polyglot.pandas;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.*;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.BabelfishEngine;

/**
 * A wrapper for a pandas series
 */
@ExportLibrary(InteropLibrary.class)
public class PandasSeriesWrapper implements TruffleObject {

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
        static Object readMember(PandasSeriesWrapper receiver, String member) throws UnsupportedMessageException, UnknownIdentifierException {
            throw UnsupportedMessageException.create();
        }
    }

    @ExportMessage
    public boolean isBfNode() {
        return true;
    }

    @ExportMessage
    public static class ExecuteBinaryOperation {

        static boolean isLoc(PandasSeriesWrapper left) {
            FieldSeries field = (FieldSeries) left;
            return field.member.equals("loc");
        }

        @Specialization(guards = "isLoc")
        public static Object executeBinaryOperation(PandasSeriesWrapper left,
                                                    Object[] right,
                                                    String member,
                                                    @Cached(value = "isLoc(left)", allowUncached = true)
                                                            boolean isLoc,
                                                    @CachedLibrary(limit = "30") InteropLibrary lib) {
            if (CompilerDirectives.inInterpreter()) {
                try {
                    ExpressionSeries expressionSeries = (ExpressionSeries) lib.readArrayElement(right[0], 0);

                    String fieldName = (String) lib.readArrayElement(right[0], 1);
                    PandasDataframeWrapper rootDataFrame = ((FieldSeries)left).df;

                    Object assignValue = right[1];
                    PandasExpression predicate = expressionSeries.expression;
                    PandasExpression elseExpression = rootDataFrame.isFieldModified(fieldName) ? rootDataFrame.getModifiedField(fieldName) : null;
                    PandasExpression.ConditionReplaceExpression expression = new PandasExpression.ConditionReplaceExpression(predicate,
                            assignValue, elseExpression, fieldName);
                    rootDataFrame.replaceField(fieldName, expression);
                } catch (UnsupportedMessageException | InvalidArrayIndexException e) {
                    e.printStackTrace();
                }

            }
            return left;
        }

        @Specialization
        public static Object executeBinaryOperation(PandasSeriesWrapper left,
                                                    Object right,
                                                    String member,
                                                    @Cached(value = "getExpressionType(member)", allowUncached = true) PandasExpression.ExpressionType expressionType) {

            PandasExpression.BinaryExpression expression = new PandasExpression.BinaryExpression(left, right, expressionType);
            return new PandasSeriesWrapper.ExpressionSeries(expression, null);
        }
    }

    static PandasExpression.ExpressionType getExpressionType(String member) {
        switch (member) {
            case "eq":
            case "__eq__":
            case "==":
                return PandasExpression.ExpressionType.EQ;
            case "ge":
            case "__ge__":
            case ">=":
                return PandasExpression.ExpressionType.GE;
            case "lt":
            case "__lt__":
            case "<":
                return PandasExpression.ExpressionType.LT;
            case "add":
            case "__add__":
            case "+":
                return PandasExpression.ExpressionType.ADD;
            case "div":
            case "__div__":
            case "__truediv__":
            case "/":
                return PandasExpression.ExpressionType.DIV;
            case "mul":
            case "__mul__":
            case "*":
                return PandasExpression.ExpressionType.MUL;
            case "|":
                return PandasExpression.ExpressionType.OR;
        }
        throw new UnsupportedOperationException(member);
    }

    @ExportMessage(name = "invokeMember")
    static class InvokeNode {


        static boolean isAssign(String member) {
            return member.equals("assign");
        }

        static boolean isSum(String member) {
            return member.equals("sum");
        }


        @Specialization(guards = {"arguments.length==1", "isAssign(cachedMember)"})
        static Object processAssignExpression(PandasSeriesWrapper receiver,
                                              String member,
                                              Object[] arguments,
                                              @Cached(value = "member", allowUncached = true) String cachedMember) {

            if (CompilerDirectives.inInterpreter()) {
                PandasDataframeWrapper rootDataFrame = ((ConditionalSeries) receiver).df;
                String fieldName = ((ConditionalSeries) receiver).field;
                Object assignValue = arguments[0];
                PandasExpression predicate = ((ConditionalSeries) receiver).condition.expression;
                PandasExpression elseExpression = rootDataFrame.isFieldModified(fieldName) ? rootDataFrame.getModifiedField(fieldName) : null;
                PandasExpression.ConditionReplaceExpression expression = new PandasExpression.ConditionReplaceExpression(predicate,
                        assignValue, elseExpression, fieldName);
                rootDataFrame.replaceField(fieldName, expression);
            }
            return receiver;
        }

        @Specialization(guards = {"arguments.length==1"})
        static PandasSeriesWrapper processBinaryExpression(PandasSeriesWrapper receiver,
                                                           String member,
                                                           Object[] arguments,
                                                           @Cached(value = "getExpressionType(member)", allowUncached = true) PandasExpression.ExpressionType expressionType) {

            PandasExpression.BinaryExpression expression = new PandasExpression.BinaryExpression(receiver, arguments[0], expressionType);
            return new PandasSeriesWrapper.ExpressionSeries(expression, null);
        }
    }

    public static class FieldSeries extends PandasSeriesWrapper {
        private final String member;
        private final PandasDataframeWrapper df;

        public FieldSeries(PandasDataframeWrapper receiver, String member) {
            this.member = member;
            this.df = receiver;
        }

        public String getMember() {
            return member;
        }
    }

    public static class ExpressionSeries extends PandasSeriesWrapper {
        private final PandasExpression expression;
        private final PandasDataframeWrapper df;

        public ExpressionSeries(PandasExpression expression, PandasDataframeWrapper df) {
            this.expression = expression;
            this.df = df;
        }

        public PandasExpression getExpression() {
            return expression;
        }
    }

    public static class ConditionalSeries extends PandasSeriesWrapper {
        private final ExpressionSeries condition;
        private final String field;
        private final PandasDataframeWrapper df;

        public ConditionalSeries(ExpressionSeries condition, String field, PandasDataframeWrapper df) {
            this.condition = condition;
            this.field = field;
            this.df = df;
        }
    }


}
