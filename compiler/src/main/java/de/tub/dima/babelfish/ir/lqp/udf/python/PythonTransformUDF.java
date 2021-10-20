package de.tub.dima.babelfish.ir.lqp.udf.python;

import de.tub.dima.babelfish.ir.lqp.udf.TransformUDF;

public class PythonTransformUDF extends PythonUDF implements TransformUDF {
    public PythonTransformUDF(String code) {
        super(code);
    }
}
