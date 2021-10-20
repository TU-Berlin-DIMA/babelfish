package de.tub.dima.babelfish.ir.lqp.udf.js;

import de.tub.dima.babelfish.ir.lqp.udf.PolyglotUDF;

public class JavaScriptUDF implements PolyglotUDF {
    private final String code;

    public JavaScriptUDF(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    @Override
    public String getLanguage() {
        return "js";
    }
}
