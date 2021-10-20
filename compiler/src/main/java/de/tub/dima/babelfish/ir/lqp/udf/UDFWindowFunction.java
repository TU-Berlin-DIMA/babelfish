package de.tub.dima.babelfish.ir.lqp.udf;

import de.tub.dima.babelfish.ir.lqp.streaming.WindowFunction;

public class UDFWindowFunction implements WindowFunction {
    private final UDF udf;

    public UDFWindowFunction(UDF udf) {
        this.udf = udf;
    }
}
