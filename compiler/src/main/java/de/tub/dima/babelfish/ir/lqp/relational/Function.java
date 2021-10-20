package de.tub.dima.babelfish.ir.lqp.relational;

import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Operator;
import de.tub.dima.babelfish.ir.lqp.schema.FieldStamp;
import de.tub.dima.babelfish.typesytem.BFType;

import java.io.Serializable;

@Operator(name = "Function")
public class Function<T extends BFType> extends LogicalOperator {


    private final Expression expression;
    private final String as;

    public Function(Expression expression, String as) {

        this.expression = expression;
        this.as = as;
    }

    public String getAs() {
        return as;
    }

    public Expression getExpression() {
        return expression;
    }

    public enum FunctionType {
        Mul,
        Div,
        Add,
        Min
    }

    public static class Expression implements Serializable {

    }

    public static class BinaryExpression extends Expression {
        private final Expression left;
        private final Expression right;
        private final FunctionType function;

        public BinaryExpression(Expression left, Expression right, FunctionType function) {
            this.left = left;
            this.right = right;
            this.function = function;
        }

        public Expression getLeft() {
            return left;
        }

        public Expression getRight() {
            return right;
        }

        public FunctionType getFunction() {
            return function;
        }
    }

    public static class ValueExpression extends Expression {
        private final FieldStamp stamp;

        public ValueExpression(FieldStamp stamp) {
            this.stamp = stamp;
        }

        public FieldStamp getStamp() {
            return stamp;
        }
    }


}
