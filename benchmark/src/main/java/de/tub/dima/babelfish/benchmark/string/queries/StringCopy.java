package de.tub.dima.babelfish.benchmark.string.queries;

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

public class StringCopy {

    static DynamicMapFunction stringCopyUDF = new DynamicMapFunction() {
        @Override
        public void map(DynamicRecord input, DynamicRecord output) {
            Text text = input.getValue("o_orderpriority");
            output.setValue("output", text);
        }
    };

    ;

    public static LogicalOperator stringCopy() {
        Scan scan = new Scan("table.orders");
        Projection projection = new Projection(new FieldReference("o_orderpriority", Text.class, 25));
        scan.addChild(projection);
        Sink sink = new Sink.MemorySink();
        projection.addChild(sink);
        return sink;
    }

    ;

    public static LogicalOperator stringCopyPython() {
        Scan scan = new Scan("table.orders");
        PythonOperator selection = new PythonOperator(new PythonUDF("def udf(rec,ctx):\n" +
                "\trec.output=rec.o_orderpriority\n" +
                "\treturn rec\n" +
                "lambda rec,ctx: udf(rec,ctx)"));
        scan.addChild(selection);
        Projection projection = new Projection(new FieldReference("output", Text.class, 25));
        selection.addChild(projection);
        Sink sink = new Sink.MemorySink();
        projection.addChild(sink);
        return sink;
    }

    ;

    public static LogicalOperator stringCopyJavaScript() {
        Scan scan = new Scan("table.orders");
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(record,ctx)=>{" +
                "record.output=record.o_orderpriority;\n" +
                "return record;" +
                "}"));
        scan.addChild(selection);
        Projection projection = new Projection(new FieldReference("output", Text.class, 25));
        selection.addChild(projection);
        Sink sink = new Sink.MemorySink();
        projection.addChild(sink);
        return sink;
    }

    public static LogicalOperator stringCopyJava() {
        Scan scan = new Scan("table.orders");
        UDFOperator selection = new JavaDynamicUDFOperator(stringCopyUDF);
        scan.addChild(selection);
        Projection projection = new Projection(new FieldReference("output", Text.class, 25));
        selection.addChild(projection);
        Sink sink = new Sink.MemorySink();
        projection.addChild(sink);
        return sink;
    }

    ;

    public static LogicalOperator getExecution(String language) {
        switch (language) {
            case "python":
                return stringCopyPython();
            case "java":
                return stringCopyJava();
            case "js":
                return stringCopyJavaScript();
            case "rel":
                return stringCopy();
        }
        throw new RuntimeException("Language: " + language + " not suported in " + StringEquals.class.getClass().getName());
    }


}
