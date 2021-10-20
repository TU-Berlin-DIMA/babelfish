package de.tub.dima.babelfish.benchmark.analytics.queries;

import de.tub.dima.babelfish.benchmark.string.queries.StringEquals;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.Aggregation;
import de.tub.dima.babelfish.ir.lqp.relational.GroupBy;
import de.tub.dima.babelfish.ir.lqp.relational.KeyGroup;
import de.tub.dima.babelfish.ir.lqp.relational.Projection;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaDynamicUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicFilterFunction;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptOperator;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptUDF;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonUDF;
import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;

public class MaxDelay {

    static DynamicFilterFunction airportEqualsUDF = new DynamicFilterFunction() {
        @Override
        public boolean filter(DynamicRecord record) {
            AbstractRope text = record.getValue("Origin");
            return text.equals(new StringText("JFK"));
        }
    };

    public static LogicalOperator maxDelayPython() {
        Scan scan = new Scan("table.airline");

        PythonOperator selection = new PythonOperator(new PythonUDF("def udf1(rec,ctx):\n" +
                "\treturn rec.Origin.equals(\"JFK\")\n" +
                //"\tctx(rec)\n" +
                "lambda rec,ctx: rec.Origin.equals(\"JFK\")"));
        scan.addChild(selection);
        Projection projection = new Projection(
                new FieldReference("DestAirportID", Int_32.class),
                new FieldReference("ArrDelay", Int_32.class)
        );
        ;
        selection.addChild(projection);
        GroupBy groupBy = new GroupBy(new KeyGroup(new FieldReference<>("DestAirportID", Int_32.class)), new Aggregation.Sum(new FieldReference<>("ArrDelay", Int_32.class)));
        projection.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator maxDelayJavaScript() {
        Scan scan = new Scan("table.airline");

        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(record,ctx)=>{" +
                "return record.Origin.equals(\"JFK\");" +
                // "ctx(record)" +
                //"}" +
                "}"));
        scan.addChild(selection);
        Projection projection = new Projection(
                new FieldReference("DestAirportID", Int_32.class),
                new FieldReference("ArrDelay", Int_32.class)
        );
        ;
        selection.addChild(projection);
        GroupBy groupBy = new GroupBy(new KeyGroup(new FieldReference<>("DestAirportID", Int_32.class)), new Aggregation.Sum(new FieldReference<>("ArrDelay", Int_32.class)));
        projection.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator maxDelayJava() {
        Scan scan = new Scan("table.airline");

        UDFOperator selection = new JavaDynamicUDFOperator(airportEqualsUDF);
        scan.addChild(selection);
        Projection projection = new Projection(
                new FieldReference("DestAirportID", Int_32.class),
                new FieldReference("ArrDelay", Int_32.class)
        );
        ;
        selection.addChild(projection);
        GroupBy groupBy = new GroupBy(new KeyGroup(new FieldReference<>("DestAirportID", Int_32.class)), new Aggregation.Sum(new FieldReference<>("ArrDelay", Int_32.class)));
        projection.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator getExecution(String language) {
        switch (language) {
            case "python":
                return maxDelayPython();
            case "java":
                return maxDelayJava();
            case "js":
                return maxDelayJavaScript();
        }
        throw new RuntimeException("Language: " + language + " not suported in " + StringEquals.class.getClass().getName());
    }


}
