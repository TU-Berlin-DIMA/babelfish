package de.tub.dima.babelfish.ir.lqp.udf;

import de.tub.dima.babelfish.ir.lqp.relational.Predicate;

public class UDFPredicate extends Predicate {

    private final UDF udf;

    public UDFPredicate(UDF udf) {
        this.udf = udf;
    }
}
