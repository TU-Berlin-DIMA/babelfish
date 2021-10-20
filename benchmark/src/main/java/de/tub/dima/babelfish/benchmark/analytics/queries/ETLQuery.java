package de.tub.dima.babelfish.benchmark.analytics.queries;

import de.tub.dima.babelfish.benchmark.string.queries.StringEquals;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.Projection;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaDynamicUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicFilterFunction;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicMapFunction;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptOperator;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptUDF;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonUDF;
import de.tub.dima.babelfish.storage.text.leaf.PointerRope;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.valueTypes.Bool;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_64;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class ETLQuery {

    static DynamicFilterFunction etlFilterUDF = new DynamicFilterFunction() {
        @Override
        public boolean filter(DynamicRecord record) {
            Bool cancelled = record.getValue("Cancelled");
            PointerRope airline = record.getValue("IATA_CODE_Reporting_Airline");
            Int_32 depDelay = record.getValue("DepDelay");
            return !cancelled.getValue() && depDelay.asInt() >= 10 && (airline.equals(new StringText("AA")) || airline.equals(new StringText("HA")));
        }
    };

    ;
    static DynamicMapFunction avgDelayUDF = new DynamicMapFunction() {
        @Override
        public void map(DynamicRecord input, DynamicRecord output) {
            Int_32 depDelay = input.getValue("DepDelay");
            Int_32 arrDelay = input.getValue("ArrDelay");
            int avgDelay = (depDelay.asInt() + arrDelay.asInt()) / 2;
            output.setValue("avgDelay", new Eager_Int_32(avgDelay));
        }
    };

    ;
    static DynamicMapFunction delayUDF = new DynamicMapFunction() {
        @Override
        public void map(DynamicRecord input, DynamicRecord output) {
            int avgDelay = ((Int_32) input.getValue("avgDelay")).asInt();
            output.setValue("avgDelay", new Eager_Int_32(avgDelay));
            if (avgDelay > 30) {
                output.setValue("delay", new StringText("High"));
            } else if (avgDelay < 20) {
                output.setValue("delay", new StringText("Low"));
            } else {
                output.setValue("delay", new StringText("Medium"));
            }
        }
    };

    public static LogicalOperator etlPython() {
        Scan scan = new Scan("table.airline");

        PythonOperator selection = new PythonOperator(new PythonUDF(
                "lambda rec,ctx: (not rec.Cancelled) and (rec.DepDelay >=10) and (rec.IATA_CODE_Reporting_Airline.equals(\"AA\") or rec.IATA_CODE_Reporting_Airline.equals(\"HA\"))"));
        scan.addChild(selection);
        Projection projection = new Projection(
                new FieldReference("IATA_CODE_Reporting_Airline", Text.class, 2),
                new FieldReference("Origin", Text.class, 6),
                new FieldReference("Dest", Text.class, 4),
                new FieldReference("DepDelay", Int_32.class),
                new FieldReference("ArrDelay", Int_32.class)
        );
        selection.addChild(projection);

        PythonOperator avgMap = new PythonOperator(new PythonUDF("def udf2(rec,ctx):\n" +
                "\trec.avgDelay = (rec.DepDelay + rec.ArrDelay) / 2 \n" +
                "\treturn rec\n" +
                "lambda a,ctx: udf2(a,ctx)"));
        projection.addChild(avgMap);
        PythonOperator delayMap = new PythonOperator(new PythonUDF("def udf3(rec,ctx):\n" +
                "\tif rec.avgDelay > 30:" +
                "\t\trec.delay = \"High\"\n" +
                "\telif rec.avgDelay < 20:\n" +
                "\t\trec.delay = \"Low\"\n" +
                "\telse:\n" +
                "\t\trec.delay = \"Medium\"\n" +
                "\treturn rec\n" +
                "lambda a,ctx: udf3(a,ctx)"));
        avgMap.addChild(delayMap);

        Projection projection2 = new Projection(
                new FieldReference("avgDelay", Float_64.class),
                new FieldReference("delay", Text.class, 6)
        );
        delayMap.addChild(projection2);
        Sink sink = new Sink.MemorySink();
        projection2.addChild(sink);
        return sink;
    }

    public static LogicalOperator etlJavaScript() {
        Scan scan = new Scan("table.airline");
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(rec,ctx)=>{" +
                "return (!rec.Cancelled && rec.DepDelay >=10 && (rec.IATA_CODE_Reporting_Airline.equals(\"AA\") || rec.IATA_CODE_Reporting_Airline.equals(\"HA\")))" +
                "" +
                "}"));
        scan.addChild(selection);
        Projection projection = new Projection(
                new FieldReference("IATA_CODE_Reporting_Airline", Text.class, 2),
                new FieldReference("Origin", Text.class, 6),
                new FieldReference("Dest", Text.class, 4),
                new FieldReference("DepDelay", Int_32.class),
                new FieldReference("ArrDelay", Int_32.class)
        );
        selection.addChild(projection);

        JavaScriptOperator avgMap = new JavaScriptOperator(new JavaScriptUDF("(rec,ctx)=>{" +
                "rec.avgDelay = (rec.DepDelay + rec.ArrDelay) / 2 ;\n" +
                "return rec;" +
                "}"));
        projection.addChild(avgMap);
        JavaScriptOperator delayMap = new JavaScriptOperator(new JavaScriptUDF("(rec,ctx)=>{" +
                "if(rec.avgDelay > 30){" +
                "rec.delay = \"High\";\n" +
                "}else if (rec.avgDelay < 20){\n" +
                "rec.delay = \"Low\";\n" +
                "}else{\n" +
                "rec.delay = \"Medium\";}\n" +
                "return rec;}"));
        avgMap.addChild(delayMap);

        Projection projection2 = new Projection(
                new FieldReference("avgDelay", Float_64.class),
                new FieldReference("delay", Text.class, 6)
        );
        delayMap.addChild(projection2);
        Sink sink = new Sink.MemorySink();
        projection2.addChild(sink);
        return sink;
    }

    public static LogicalOperator etlJava() {
        Scan scan = new Scan("table.airline");
        UDFOperator selection = new JavaDynamicUDFOperator(etlFilterUDF);
        scan.addChild(selection);
        Projection projection = new Projection(
                new FieldReference("IATA_CODE_Reporting_Airline", Text.class, 2),
                new FieldReference("Origin", Text.class, 6),
                new FieldReference("Dest", Text.class, 4),
                new FieldReference("DepDelay", Int_32.class),
                new FieldReference("ArrDelay", Int_32.class)
        );
        selection.addChild(projection);

        UDFOperator avgMap = new JavaDynamicUDFOperator(avgDelayUDF);
        projection.addChild(avgMap);
        UDFOperator delayMap = new JavaDynamicUDFOperator(delayUDF);
        avgMap.addChild(delayMap);

        Projection projection2 = new Projection(
                new FieldReference("avgDelay", Float_64.class),
                new FieldReference("delay", Text.class, 6)
        );
        delayMap.addChild(projection2);
        Sink sink = new Sink.MemorySink();
        projection2.addChild(sink);
        return sink;
    }

    ;

    public static LogicalOperator getExecution(String language) {
        switch (language) {
            case "python":
                return etlPython();
            case "java":
                return etlJava();
            case "js":
                return etlJavaScript();
        }
        throw new RuntimeException("Language: " + language + " not suported in " + StringEquals.class.getClass().getName());
    }

}
