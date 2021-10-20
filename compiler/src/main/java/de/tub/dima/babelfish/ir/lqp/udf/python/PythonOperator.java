package de.tub.dima.babelfish.ir.lqp.udf.python;

import de.tub.dima.babelfish.ir.lqp.udf.PolyglotUDF;
import de.tub.dima.babelfish.ir.lqp.udf.PolyglotUDFOperator;

public class PythonOperator extends PolyglotUDFOperator {


    public PythonUDF udf;

    public PythonOperator(PythonUDF udf) {

        this.udf = udf;
    }

    @Override
    public PolyglotUDF getUdf() {
        return udf;
    }
}
