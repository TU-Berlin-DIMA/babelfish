package de.tub.dima.babelfish.ir.lqp.udf;

public interface PolyglotUDF extends UDF {
    String getCode();

    String getLanguage();
}
