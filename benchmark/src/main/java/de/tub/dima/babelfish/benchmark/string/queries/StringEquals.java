package de.tub.dima.babelfish.benchmark.string.queries;

import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.Projection;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaDynamicUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicFilterFunction;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptOperator;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptUDF;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonUDF;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class StringEquals {

    static DynamicFilterFunction stringEqualsUDF = new DynamicFilterFunction() {
        @Override
        public boolean filter(DynamicRecord record) {
            Text text = record.getValue("o_orderpriority");
            return text.equals(new StringText("5-LOW", 15));
        }
    };

    ;

    public static LogicalOperator stringEquals() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(new FieldReference("o_orderpriority", Text.class, 25));
        scan.addChild(projection);
        PythonOperator selection = new PythonOperator(new PythonUDF("def udf(rec,ctx):\n" +
                //"\tif :\n" +
                //"\t\tctx(rec)\n" +
                "lambda rec,ctx: rec.o_orderpriority.equals(\"5-LOW\")"));

        projection.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    public static LogicalOperator stringEqualsJava() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(new FieldReference("o_orderpriority", Text.class, 25));
        scan.addChild(projection);
        UDFOperator selection = new JavaDynamicUDFOperator(stringEqualsUDF);
        projection.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    ;

    public static LogicalOperator stringEqualsPython() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(new FieldReference("o_orderpriority", Text.class, 25));
        scan.addChild(projection);
        PythonOperator selection = new PythonOperator(new PythonUDF("def udf(rec,ctx):\n" +
                "\tif rec.o_orderpriority.equals(\"5-LOW\"):\n" +
                "\t\tctx(rec)\n" +
                "lambda rec,ctx: rec.o_orderpriority.equals(\"5-LOW\")"));
        projection.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    ;

    public static LogicalOperator stringEqualsJavaScript() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(new FieldReference("o_orderpriority", Text.class, 25));
        scan.addChild(projection);
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(record,ctx)=>{" +
                "return record.o_orderpriority.equals(\"5-LOW\");\n" +
                "}"));
        projection.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    ;

    public static LogicalOperator getExecution(String language) {
        switch (language) {
            case "python":
                return stringEqualsPython();
            case "java":
                return stringEqualsJava();
            case "js":
                return stringEqualsJavaScript();
            case "rel":
                return stringEquals();
        }
        throw new RuntimeException("Language: " + language + " not suported in " + StringEquals.class.getClass().getName());
    }

}
