package de.tub.dima.babelfish.ir.lqp.udf.js;


import de.tub.dima.babelfish.ir.lqp.udf.PolyglotUDF;
import de.tub.dima.babelfish.ir.lqp.udf.PolyglotUDFOperator;

public class JavaScriptOperator extends PolyglotUDFOperator {


    public JavaScriptUDF udf;

    public JavaScriptOperator(JavaScriptUDF udf) {

        this.udf = udf;
    }

    @Override
    public PolyglotUDF getUdf() {
        return udf;
    }
}
