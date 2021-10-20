package de.tub.dima.babelfish.ir.lqp.udf.python;

import de.tub.dima.babelfish.ir.lqp.udf.ScalarUDF;

public class PythonScalarUDF extends PythonUDF implements ScalarUDF {
    public PythonScalarUDF(String code) {
        super(code);
    }
}
