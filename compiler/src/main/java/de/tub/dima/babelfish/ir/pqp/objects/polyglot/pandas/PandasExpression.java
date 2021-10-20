package de.tub.dima.babelfish.ir.pqp.objects.polyglot.pandas;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.nodes.RootNode;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.records.ConditionalFieldUpdate;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.map.ExpressionNodeFactory;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.selection.PredicateNode;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.selection.PredicateNodeFactory;

/**
 * Expressions to operate on pandas series
 */
@ExportLibrary(value = InteropLibrary.class)
public abstract class PandasExpression implements TruffleObject {
    public abstract BFExecutableNode createPredicateNode(FrameDescriptor frameDescriptor, TruffleLanguage language);

    enum ExpressionType {
        EQ,
        LT,
        GT,
        GE,
        ADD,
        DIV,
        OR,
        MUL,
        CON_REPLACE
    }

    public static class ConditionReplaceExpression extends PandasExpression {
        private final PandasExpression predicate;
        private final Object trueExpression;
        private final Object elseExpression;
        private final String member;

        public ConditionReplaceExpression(PandasExpression predicate, Object expression, Object elseExpression, String member) {
            this.predicate = predicate;
            this.trueExpression = expression;
            this.elseExpression = elseExpression;
            this.member = member;
        }

        @Override
        public BFExecutableNode createPredicateNode(FrameDescriptor frameDescriptor, TruffleLanguage language) {
            BFExecutableNode predicateNode = predicate.createPredicateNode(frameDescriptor, language);
            BFExecutableNode trueExpressionItem = getItem(trueExpression, frameDescriptor, language);
            BFExecutableNode elseExpressionItem = getItem(elseExpression, frameDescriptor, language);
            return new ConditionalFieldUpdate(frameDescriptor, (PredicateNode) predicateNode, trueExpressionItem, elseExpressionItem, member);
        }
    }

    public static class BinaryExpression extends PandasExpression {
        private final Object leftValue;
        private final Object rightValue;
        private final ExpressionType expressionType;

        public BinaryExpression(Object leftValue, Object rightValue, ExpressionType expressionType) {
            this.leftValue = leftValue;
            this.rightValue = rightValue;
            this.expressionType = expressionType;
        }

        @Override
        public BFExecutableNode createPredicateNode(FrameDescriptor frameDescriptor, TruffleLanguage language) {
            BFExecutableNode left = getItem(leftValue, frameDescriptor, language);
            BFExecutableNode right = getItem(rightValue, frameDescriptor, language);
            switch (expressionType) {
                case EQ:
                    return PredicateNodeFactory.EqualNodeGen.create(left, right);
                case GE:
                    return PredicateNodeFactory.GreaterEqualsNodeGen.create(left, right);
                case LT:
                    return PredicateNodeFactory.LessThanNodeGen.create(left, right);
                case OR:
                    return new PredicateNode.OrNode((PredicateNode) left, (PredicateNode) right);
                case ADD:
                    return ExpressionNodeFactory.AddExpressionNodeGen.create(language, left, right);
                case DIV:
                    return ExpressionNodeFactory.DivExpressionNodeGen.create(language, left, right);
                case MUL:
                    return ExpressionNodeFactory.MulExpressionNodeGen.create(language, left, right);
            }
            return null;
        }
    }

    @ExportMessage
    public static boolean isBfNode(PandasExpression left) {
        return true;
    }

    @ExportMessage
    public static class ExecuteBinaryOperation {
        @Specialization()
        public static Object executeBinaryOperation(PandasExpression left,
                                                    PandasExpression right,
                                                    String member,
                                                    @Cached(value = "member", allowUncached = true) String cachedMember) {
            //PandasExpression.BinaryExpression expression = new PandasExpression.BinaryExpression(left.getExpression(), right.getExpression(), PandasExpression.ExpressionType.OR);
           // return new PandasSeriesWrapper.ExpressionSeries(expression);
            return left;
        }
    }

    public BFExecutableNode getItem(Object object,
                                    FrameDescriptor frameDescriptor,
                                    TruffleLanguage language) {
        if (object instanceof PandasSeriesWrapper.FieldSeries) {
            return new PredicateNode.ReadFieldNode(((PandasSeriesWrapper.FieldSeries) object).getMember(), frameDescriptor, language);
        } else if (object instanceof BinaryExpression) {
            return ((BinaryExpression) object).createPredicateNode(frameDescriptor, language);
        } else if (object instanceof PandasSeriesWrapper.ExpressionSeries) {
            return ((PandasSeriesWrapper.ExpressionSeries) object).getExpression().createPredicateNode(frameDescriptor, language);
        } else if (object instanceof ConditionReplaceExpression) {
            return ((ConditionReplaceExpression) object).createPredicateNode(frameDescriptor, language);
        }else {
            return PredicateNodeFactory.ConstantValueNodeGen.create(language, object);
        }
    }

    public static class EvalRootNode extends RootNode {

        @Child
        private BFExecutableNode predicateNode;
        private final FrameSlot inputObjectSlot;

        public EvalRootNode(TruffleLanguage lang, FrameDescriptor frameDescriptor, BFExecutableNode predicateNode) {
            super(lang, frameDescriptor);
            this.predicateNode = predicateNode;
            inputObjectSlot = frameDescriptor.findOrAddFrameSlot("object", FrameSlotKind.Object);
        }

        @Override
        public Object execute(VirtualFrame frame) {
            Object object = frame.getArguments()[0];
            frame.setObject(inputObjectSlot, object);
            return predicateNode.execute(frame);
        }
    }
}
