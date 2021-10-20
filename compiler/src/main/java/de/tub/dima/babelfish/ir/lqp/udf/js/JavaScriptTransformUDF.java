package de.tub.dima.babelfish.ir.lqp.udf.js;

import de.tub.dima.babelfish.ir.lqp.udf.TransformUDF;

public class JavaScriptTransformUDF extends JavaScriptUDF implements TransformUDF {

    public JavaScriptTransformUDF(String code) {
        super(code);
    }
}
