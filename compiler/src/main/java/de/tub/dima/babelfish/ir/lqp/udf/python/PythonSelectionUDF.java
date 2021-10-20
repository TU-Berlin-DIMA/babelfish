package de.tub.dima.babelfish.ir.lqp.udf.python;

import de.tub.dima.babelfish.ir.lqp.udf.SelectionUDF;

public class PythonSelectionUDF extends PythonUDF implements SelectionUDF {
    public PythonSelectionUDF(String code) {
        super(code);
    }
}
