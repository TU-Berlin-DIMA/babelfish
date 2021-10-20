package de.tub.dima.babelfish.ir.lqp.relational;

import de.tub.dima.babelfish.ir.lqp.LogicalPlanNode;
import de.tub.dima.babelfish.ir.lqp.schema.FieldStamp;
import de.tub.dima.babelfish.typesytem.BFType;

public abstract class Predicate<T extends BFType> implements LogicalPlanNode {


    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    public static abstract class BinaryPredicate extends Predicate {
        private final Predicate left;
        private final Predicate right;

        protected BinaryPredicate(Predicate left, Predicate right) {
            this.left = left;
            this.right = right;
        }

        public Predicate getLeft() {
            return left;
        }

        public Predicate getRight() {
            return right;
        }
    }

    public static class And extends BinaryPredicate {

        public And(Predicate left, Predicate right) {
            super(left, right);
        }
    }

    public static class Or extends BinaryPredicate {

        public Or(Predicate left, Predicate right) {
            super(left, right);
        }
    }

    public static class Condition<T extends BFType> extends Predicate<T> {

        private final FieldStamp<T> left;
        private final FieldStamp<T> right;

        protected Condition(FieldStamp<T> left, FieldStamp<T> right) {
            this.left = left;
            this.right = right;
        }

        public FieldStamp<T> getLeft() {
            return left;
        }

        public FieldStamp<T> getRight() {
            return right;
        }
    }

    public static class Equal<T extends BFType> extends Condition<T> {

        public Equal(FieldStamp<T> left, FieldStamp<T> right) {
            super(left, right);
        }
    }

    public static class NotEqual<T extends BFType> extends Condition<T> {

        public NotEqual(FieldStamp<T> left, FieldStamp<T> right) {
            super(left, right);
        }
    }

    public static class LessThen<T extends BFType> extends Condition<T> {

        public LessThen(FieldStamp<T> left, FieldStamp<T> right) {
            super(left, right);
        }
    }

    public static class LessEquals<T extends BFType> extends Condition<T> {

        public LessEquals(FieldStamp<T> left, FieldStamp<T> right) {
            super(left, right);
        }
    }

    public static class GreaterThan<T extends BFType> extends Condition<T> {

        public GreaterThan(FieldStamp<T> left, FieldStamp<T> right) {
            super(left, right);
        }
    }

    public static class GreaterEquals<T extends BFType> extends Condition<T> {

        public GreaterEquals(FieldStamp<T> left, FieldStamp<T> right) {
            super(left, right);
        }
    }
}
