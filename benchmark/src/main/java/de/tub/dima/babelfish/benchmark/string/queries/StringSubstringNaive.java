package de.tub.dima.babelfish.benchmark.string.queries;

import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.Projection;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaDynamicUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicMapFunction;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptOperator;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptUDF;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonUDF;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class StringSubstringNaive {


    static DynamicMapFunction stringSubstringUDF = new DynamicMapFunction() {
        @Override
        public void map(DynamicRecord input, DynamicRecord output) {
            String text = input.getString("o_orderpriority");
            output.setValue("o_orderpriority", text.substring(2, 5));
        }
    };

    ;

    public static LogicalOperator stringSubstringPython() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(new FieldReference("o_orderpriority", Text.class, 25));
        scan.addChild(projection);
        PythonOperator selection = new PythonOperator(new PythonUDF("def udf(rec,ctx):\n" +
                "\trec.o_orderpriority = rec.o_orderpriority[2:5]\n" +
                "\treturn rec\n" +
                "lambda a,ctx: udf(a,ctx)"));
        projection.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    ;

    public static LogicalOperator stringSubstringJava() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(new FieldReference("o_orderpriority", Text.class, 25));
        scan.addChild(projection);
        UDFOperator selection = new JavaDynamicUDFOperator(stringSubstringUDF);
        projection.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    public static LogicalOperator stringSubstringJavaScript() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(new FieldReference("o_orderpriority", Text.class, 25));
        scan.addChild(projection);
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(record,ctx)=>{" +
                "record.o_orderpriority = record.o_orderpriority.substring(2,5);" +
                "return record;" +
                "}"));
        projection.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    ;

    public static LogicalOperator getExecution(String language) {
        RuntimeConfiguration.NAIVE_STRING_HANDLING = true;
        switch (language) {
            case "python":
                return stringSubstringPython();
            case "java":
                return stringSubstringJava();
            case "js":
                return stringSubstringJavaScript();
        }
        throw new RuntimeException("Language: " + language + " not suported in " + StringEquals.class.getClass().getName());
    }

}