package de.tub.dima.babelfish.ir.lqp.relational;

import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Operator;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;

@Operator(name = "Projection")
public class Projection extends LogicalOperator {

    private final FieldReference[] fieldReferences;

    public Projection(FieldReference... fieldReferences) {
        this.fieldReferences = fieldReferences;
    }

    public FieldReference[] getFieldReferences() {
        return fieldReferences;
    }
}
