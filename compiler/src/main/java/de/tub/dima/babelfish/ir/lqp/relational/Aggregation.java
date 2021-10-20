package de.tub.dima.babelfish.ir.lqp.relational;

import de.tub.dima.babelfish.ir.lqp.LogicalPlanNode;
import de.tub.dima.babelfish.ir.lqp.Operator;
import de.tub.dima.babelfish.ir.lqp.schema.FieldStamp;
import de.tub.dima.babelfish.typesytem.valueTypes.ValueType;
import de.tub.dima.babelfish.typesytem.valueTypes.number.NumberType;


public abstract class Aggregation implements LogicalPlanNode {

    private final FieldStamp fieldStamp;

    protected Aggregation(FieldStamp fieldStamp) {
        this.fieldStamp = fieldStamp;
    }

    public FieldStamp getFieldStamp() {
        return fieldStamp;
    }

    @Operator(name = "Sum")
    public static class Sum extends Aggregation {
        public Sum(FieldStamp<? extends ValueType> fieldStamp) {
            super(fieldStamp);
        }
    }

    @Operator(name = "Avg")
    public static class Avg extends Aggregation {
        public Avg(FieldStamp<? extends NumberType> fieldStamp) {
            super(fieldStamp);
        }
    }

    @Operator(name = "Keep")
    public static class Keep extends Aggregation {
        public Keep(FieldStamp fieldStamp) {
            super(fieldStamp);
        }
    }

    @Operator(name = "Count")
    public static class Count extends Aggregation {
        public Count() {
            super(null);
        }
    }

    @Operator(name = "Min")
    public static class Min extends Aggregation {
        public Min(FieldStamp<? extends ValueType> fieldStamp) {
            super(fieldStamp);
        }
    }
}
