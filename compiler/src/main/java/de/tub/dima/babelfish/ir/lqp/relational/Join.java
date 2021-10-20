package de.tub.dima.babelfish.ir.lqp.relational;

import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Operator;
import de.tub.dima.babelfish.ir.lqp.schema.FieldStamp;
import de.tub.dima.babelfish.typesytem.BFType;

@Operator(name = "Join")
public abstract class Join extends LogicalOperator {

    private final long cardinality;
    private final Predicate predicate;

    protected Join(long cardinality, Predicate predicate) {
        this.cardinality = cardinality;
        this.predicate = predicate;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public long getCardinality() {
        return cardinality;
    }

    public static class EqualJoin<T extends BFType> extends Join {

        public EqualJoin(FieldStamp<T> left, FieldStamp<T> right) {
            super(-1, new Predicate.Equal<>(left, right));
        }

        public EqualJoin(long cardinality, FieldStamp<T> left, FieldStamp<T> right) {
            super(cardinality, new Predicate.Equal<>(left, right));
        }
    }
}
