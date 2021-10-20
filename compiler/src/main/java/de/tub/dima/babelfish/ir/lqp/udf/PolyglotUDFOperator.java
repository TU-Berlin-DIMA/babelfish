package de.tub.dima.babelfish.ir.lqp.udf;

import de.tub.dima.babelfish.ir.lqp.LogicalOperator;

public abstract class PolyglotUDFOperator extends LogicalOperator {

    public abstract PolyglotUDF getUdf();
}
