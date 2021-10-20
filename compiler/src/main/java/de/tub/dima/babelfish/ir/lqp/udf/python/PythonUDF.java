package de.tub.dima.babelfish.ir.lqp.udf.python;

import de.tub.dima.babelfish.ir.lqp.udf.PolyglotUDF;

public class PythonUDF implements PolyglotUDF {
    private final String code;

    public PythonUDF(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    @Override
    public String getLanguage() {
        return "python";
    }
}
