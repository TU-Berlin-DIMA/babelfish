package de.tub.dima.babelfish.ir.lqp.udf.js;

import de.tub.dima.babelfish.ir.lqp.udf.ScalarUDF;

public class JavaScriptScalarUDF extends JavaScriptUDF implements ScalarUDF {

    public JavaScriptScalarUDF(String code) {
        super(code);
    }
}
