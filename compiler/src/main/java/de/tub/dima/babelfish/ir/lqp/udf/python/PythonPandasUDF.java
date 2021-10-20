package de.tub.dima.babelfish.ir.lqp.udf.python;

import de.tub.dima.babelfish.ir.lqp.udf.ScalarUDF;

public class PythonPandasUDF extends PythonUDF implements ScalarUDF {
    public PythonPandasUDF(String code) {
        super(code);
    }
}
