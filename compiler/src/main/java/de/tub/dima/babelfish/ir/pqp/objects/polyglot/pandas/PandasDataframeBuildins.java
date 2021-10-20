package de.tub.dima.babelfish.ir.pqp.objects.polyglot.pandas;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.CachedLanguage;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnknownIdentifierException;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.nodes.DirectCallNode;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.polyglot.DataFrameScanNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;

/**
 * This class defines build in function for pandas data frames
 */
@ExportLibrary(value = InteropLibrary.class, receiverType = PandasDataframeWrapper.class)
@ExportLibrary(value = PandasDataframeLibrary.class, receiverType = PandasDataframeWrapper.class)
public abstract class PandasDataframeBuildins {

    @ExportMessage(library = PandasDataframeLibrary.class)
    static public class Open {
        public static DataFrameScanNode getScanNode(PandasDataframeWrapper.PandasRootDataFrame df) {
            return new DataFrameScanNode(df.getRelation());
        }

        @Specialization
        public static void open(PandasDataframeWrapper.PandasRootDataFrame df,
                                @Cached(value = "getScanNode(df)", allowUncached = true) DataFrameScanNode scan) {
            scan.open(df.child);
        }

        @Specialization
        public static void open(PandasDataframeWrapper df,
                                @CachedLibrary(limit = "30") PandasDataframeLibrary lib) {
            lib.open(df.parent);
        }
    }

    @ExportMessage
    public static boolean isBfNode(PandasDataframeWrapper left) {
        return true;
    }

    @ExportMessage
    public static class ExecuteBinaryOperation {
        @Specialization(guards = "isFilter(cachedMember)")
        public static Object executeBinaryOperation(PandasDataframeWrapper left,
                                                    PandasSeriesWrapper.ExpressionSeries rigt,
                                                    String member,
                                                    @Cached(value = "member", allowUncached = true) String cachedMember) {
            PandasDataframeWrapper.PandasFilteredDataFrame filtered = new PandasDataframeWrapper.PandasFilteredDataFrame(left, rigt.getExpression());
            left.child = filtered;
            return filtered;
        }

        @Specialization(guards = {"isFilter(cachedMember)", "lib.hasArrayElements(right)"})
        public static Object executeBinaryOperation(PandasDataframeWrapper left,
                                                    Object right,
                                                    String member,
                                                    @Cached(value = "member", allowUncached = true) String cachedMember,
                                                    @CachedLibrary(limit = "30") InteropLibrary lib) {

            try {
                for (int i = 0; i < lib.getArraySize(right); i++) {
                    left.selectField((String) lib.readArrayElement(right, i));
                }
            } catch (Exception e) {
            }
            return left;
        }
    }


    @ExportMessage(library = PandasDataframeLibrary.class)
    static public class Execute {

        static DirectCallNode getPredicate(TruffleLanguage.LanguageReference<BabelfishEngine> lang, PandasDataframeWrapper.PandasFilteredDataFrame df) {
            FrameDescriptor fr = new FrameDescriptor();
            BFExecutableNode predicate = df.createPredicateNode(lang.get(), fr);
            PandasExpression.EvalRootNode root = new PandasExpression.EvalRootNode(lang.get(), fr, predicate);
            return Truffle.getRuntime().createDirectCallNode(Truffle.getRuntime().createCallTarget(root));
        }

        @Specialization
        public static void execute(
                PandasDataframeWrapper.PandasFilteredDataFrame df,
                BFRecord bfRecord,
                @CachedLibrary(limit = "30") PandasDataframeLibrary lib,
                @CachedLanguage TruffleLanguage.LanguageReference<BabelfishEngine> languageRef,
                @Cached(value = "getPredicate(languageRef,df)", allowUncached = true) DirectCallNode predicateCallNode) {
            if ((boolean) predicateCallNode.call(bfRecord)) {
                lib.execute(df.child, bfRecord);
            }
        }

        @Specialization
        public static void execute(
                PandasDataframeWrapper.CountDataFrame df,
                BFRecord bfRecord) {
            df.count = df.count + 1;
        }

        public static DirectCallNode getNextOperator(PandasDataframeWrapper.ExecuteNextOperator df) {
            return Truffle.getRuntime().createDirectCallNode(Truffle.getRuntime().createCallTarget(
                    df.nextOperator.getExecuteCall())
            );
        }

        public static TranslateToBFRecordFromPandas getTranslateOperator() {
            return new TranslateToBFRecordFromPandas();
        }

        @Specialization
        public static void execute(
                PandasDataframeWrapper.ExecuteNextOperator df,
                BFRecord bfRecord,
                @Cached(value = "getNextOperator(df)", allowUncached = true) DirectCallNode nextOperator,
                @Cached(value = "getTranslateOperator()", allowUncached = true) TranslateToBFRecordFromPandas translate) {
            BFRecord resultRecord = translate.translate(df, bfRecord);
            nextOperator.call(resultRecord, df.stateManager);
        }
    }


    @ExportMessage
    static protected boolean hasMembers(PandasDataframeWrapper receiver) {
        return true;
    }

    @ExportMessage
    static protected Object getMembers(PandasDataframeWrapper receiver, boolean includeInternal) {
        return true;
    }

    @ExportMessage
    static protected boolean isMemberReadable(PandasDataframeWrapper receiver, String string) {
        return true;
    }

    @ExportMessage
    static protected boolean isMemberInvocable(PandasDataframeWrapper receiver, String string) {
        return true;
    }

    @ExportMessage
    static public boolean hasLanguage(PandasDataframeWrapper receiver) {
        return true;
    }

    @ExportMessage
    static public String toDisplayString(PandasDataframeWrapper receiver, boolean allowSideEffects) {
        return "";
    }

    @ExportMessage
    static public Class<? extends TruffleLanguage<?>> getLanguage(PandasDataframeWrapper receiver) {
        return BabelfishEngine.class;
    }

    @ExportMessage(name = "readMember")
    static class ReadNode {

        public static boolean isFieldModified(PandasDataframeWrapper receiver, String member) {
            return receiver.isFieldModified(member);
        }

        public static PandasExpression getModifiedField(PandasDataframeWrapper receiver, String member) {
            return receiver.getModifiedField(member);
        }

        @Specialization(guards = "!isModified")
        static PandasSeriesWrapper readUnmodified(PandasDataframeWrapper receiver,
                                                  String member,
                                                  @Cached(value = "isFieldModified(receiver, member)", allowUncached = true) boolean isModified) throws UnsupportedMessageException, UnknownIdentifierException {
            return new PandasSeriesWrapper.FieldSeries(receiver, member);
        }

        @Specialization(guards = "isModified")
        static PandasSeriesWrapper readModified(PandasDataframeWrapper receiver,
                                                String member,
                                                @Cached(value = "isFieldModified(receiver, member)", allowUncached = true) boolean isModified,
                                                @Cached(value = "getModifiedField(receiver, member)", allowUncached = true) PandasExpression expression) throws UnsupportedMessageException, UnknownIdentifierException {
            return new PandasSeriesWrapper.ExpressionSeries(expression, receiver);
        }
    }

    @ExportMessage
    static boolean isMemberModifiable(PandasDataframeWrapper receiver, String member) {
        return true;
    }

    @ExportMessage
    static boolean isMemberInsertable(PandasDataframeWrapper receiver, String member) {
        return true;
    }

    @ExportMessage(name = "writeMember")
    static class WriteNode {

        @Specialization
        static void writeMember(PandasDataframeWrapper receiver, String member, PandasSeriesWrapper.ExpressionSeries expression) throws UnsupportedMessageException, UnknownIdentifierException {
            CompilerDirectives.interpreterOnly(() -> {
                receiver.replaceField(member, expression.getExpression());
            });
        }
    }

    static boolean isFilter(String member) {
        return member.equals("filter") || member.equals("___getitem___");
    }

    @ExportMessage(name = "invokeMember")
    static class InvokeNode {


        static boolean isLoc(String member) {
            return member.equals("loc");
        }

        static boolean isCount(String member) {
            return member.equals("count");
        }

        static boolean isProject(String member) {
            return member.equals("project");
        }

        @Specialization(guards = "isFilter(cachedMember)")
        static PandasDataframeWrapper filter(PandasDataframeWrapper receiver, String member, Object[] expression,
                                             @Cached(value = "member", allowUncached = true) String cachedMember) {
            PandasSeriesWrapper.ExpressionSeries expressionSeries = (PandasSeriesWrapper.ExpressionSeries) expression[0];
            PandasDataframeWrapper.PandasFilteredDataFrame filtered = new PandasDataframeWrapper.PandasFilteredDataFrame(receiver, expressionSeries.getExpression());
            receiver.child = filtered;
            return filtered;
        }

        @Specialization(guards = "isLoc(cachedMember)")
        static PandasSeriesWrapper.ConditionalSeries loc(PandasDataframeWrapper receiver, String member, Object[] expression,
                                                         @Cached(value = "member", allowUncached = true) String cachedMember) {
            PandasSeriesWrapper.ExpressionSeries expressionSeries = (PandasSeriesWrapper.ExpressionSeries) expression[0];
            String field = (String) expression[1];
            return new PandasSeriesWrapper.ConditionalSeries(expressionSeries, field, receiver);
        }

        @Specialization(guards = "isCount(cachedMember)")
        static int count(PandasDataframeWrapper receiver, String member, Object[] expression,
                         @Cached(value = "member", allowUncached = true) String cachedMember,
                         @CachedLibrary(value = "receiver") PandasDataframeLibrary lib) {
            PandasDataframeWrapper.CountDataFrame count = new PandasDataframeWrapper.CountDataFrame(receiver);
            receiver.child = count;
            lib.open(receiver);
            return count.count;
        }

        @Specialization(guards = "isProject(cachedMember)")
        static PandasDataframeWrapper project(PandasDataframeWrapper receiver, String member, Object[] fields,
                                              @Cached(value = "member", allowUncached = true) String cachedMember,
                                              @CachedLibrary(value = "receiver") PandasDataframeLibrary lib) {
            for (Object object : fields) {
                receiver.selectField(object.toString());
            }
            return receiver;
        }

    }
}
