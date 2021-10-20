package de.tub.dima.babelfish.benchmark.tcph.queries;

import de.tub.dima.babelfish.benchmark.string.queries.StringEquals;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.*;
import de.tub.dima.babelfish.ir.lqp.schema.FieldConstant;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaDynamicUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicFilterFunction;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptOperator;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptSelectionUDF;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonSelectionUDF;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.udt.LazyDate;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;

public class Query1 {

    static DynamicFilterFunction tcphfilterTypedUDF = new DynamicFilterFunction() {
        @Override
        public boolean filter(DynamicRecord record) {
            LazyDate l_shipdate = record.getValue("l_shipdate");
            return l_shipdate.before("1998-09-02");
        }
    };

    public static LogicalOperator relationalQueryTCPH1() {
        Scan scan = new Scan("table.lineitem");
        Selection selection = new Selection(new Predicate.LessThen<>(
                new FieldReference<>("l_shipdate", Date.class),
                new FieldConstant<>(new Date("1998-09-02"))));

        scan.addChild(selection);
        Function<?> function1 = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.BinaryExpression(
                                new Function.ValueExpression(new FieldConstant<>(new EagerNumeric(1, 2))),
                                new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class, 2)),
                                Function.FunctionType.Min),
                        Function.FunctionType.Mul
                ), "disc_price");
        selection.addChild(function1);
        Function<?> function2 = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("disc_price", Numeric.class, 4)),
                        new Function.BinaryExpression(
                                new Function.ValueExpression(new FieldConstant<>(new EagerNumeric(1, 2))),
                                new Function.ValueExpression(new FieldReference<>("l_tax", Numeric.class, 2)),
                                Function.FunctionType.Add),
                        Function.FunctionType.Mul
                ), "charge");
        function1.addChild(function2);
        GroupBy groupBy =
                new GroupBy(
                        new KeyGroup(
                                new FieldReference("l_returnflag", Char.class),
                                new FieldReference("l_linestatus", Char.class)),
                        new Aggregation.Count(),
                        new Aggregation.Sum(new FieldReference("l_quantity", Numeric.class, 2)),
                        new Aggregation.Sum(new FieldReference("l_extendedprice", Numeric.class, 2)),
                        new Aggregation.Sum(new FieldReference("disc_price", Numeric.class, 4)),
                        new Aggregation.Sum(new FieldReference("charge", Numeric.class, 6))
                );
        function2.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);

        return sink;
    }

    public static LogicalOperator pythonQueryTCPH1() {
        Scan scan = new Scan("table.lineitem");

        PythonOperator selection = new PythonOperator(new PythonSelectionUDF(
                "lambda rec,ctx:  rec.l_shipdate.before(\"1998-09-02\")"));
        scan.addChild(selection);
        Function<?> function1 = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.BinaryExpression(
                                new Function.ValueExpression(new FieldConstant<>(new EagerNumeric(1, 2))),
                                new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class, 2)),
                                Function.FunctionType.Min),
                        Function.FunctionType.Mul
                ), "disc_price");
        selection.addChild(function1);
        Function<?> function2 = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("disc_price", Numeric.class, 4)),
                        new Function.BinaryExpression(
                                new Function.ValueExpression(new FieldConstant<>(new EagerNumeric(1, 2))),
                                new Function.ValueExpression(new FieldReference<>("l_tax", Numeric.class, 2)),
                                Function.FunctionType.Add),
                        Function.FunctionType.Mul
                ), "charge");
        function1.addChild(function2);
        GroupBy groupBy =
                new GroupBy(
                        new KeyGroup(
                                new FieldReference("l_returnflag", Char.class),
                                new FieldReference("l_linestatus", Char.class)),
                        new Aggregation.Count(),
                        new Aggregation.Sum(new FieldReference("l_quantity", Numeric.class, 2)),
                        new Aggregation.Sum(new FieldReference("l_extendedprice", Numeric.class, 2)),
                        new Aggregation.Sum(new FieldReference("disc_price", Numeric.class, 4)),
                        new Aggregation.Sum(new FieldReference("charge", Numeric.class, 6))
                );
        function2.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);

        return sink;
    }

    public static LogicalOperator javaScriptQueryTCPH1() {
        Scan scan = new Scan("table.lineitem");
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptSelectionUDF("(record,ctx)=>{" +
                "return record.l_shipdate.before(\"1998-09-02\")" +
                "}"));

        scan.addChild(selection);
        Function<?> function1 = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.BinaryExpression(
                                new Function.ValueExpression(new FieldConstant<>(new EagerNumeric(1, 2))),
                                new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class, 2)),
                                Function.FunctionType.Min),
                        Function.FunctionType.Mul
                ), "disc_price");
        selection.addChild(function1);
        Function<?> function2 = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("disc_price", Numeric.class, 4)),
                        new Function.BinaryExpression(
                                new Function.ValueExpression(new FieldConstant<>(new EagerNumeric(1, 2))),
                                new Function.ValueExpression(new FieldReference<>("l_tax", Numeric.class, 2)),
                                Function.FunctionType.Add),
                        Function.FunctionType.Mul
                ), "charge");
        function1.addChild(function2);
        GroupBy groupBy =
                new GroupBy(
                        new KeyGroup(
                                new FieldReference("l_returnflag", Char.class),
                                new FieldReference("l_linestatus", Char.class)),
                        new Aggregation.Count(),
                        new Aggregation.Sum(new FieldReference("l_quantity", Numeric.class, 2)),
                        new Aggregation.Sum(new FieldReference("l_extendedprice", Numeric.class, 2)),
                        new Aggregation.Sum(new FieldReference("disc_price", Numeric.class, 4)),
                        new Aggregation.Sum(new FieldReference("charge", Numeric.class, 6))
                );
        function2.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);

        return sink;
    }

    public static LogicalOperator javaQueryTCPH1() {
        Scan scan = new Scan("table.lineitem");
        UDFOperator selection = new JavaDynamicUDFOperator(tcphfilterTypedUDF);
        scan.addChild(selection);
        Function<?> function1 = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.BinaryExpression(
                                new Function.ValueExpression(new FieldConstant<>(new EagerNumeric(1, 2))),
                                new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class, 2)),
                                Function.FunctionType.Min),
                        Function.FunctionType.Mul
                ), "disc_price");
        selection.addChild(function1);
        Function<?> function2 = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("disc_price", Numeric.class, 4)),
                        new Function.BinaryExpression(
                                new Function.ValueExpression(new FieldConstant<>(new EagerNumeric(1, 2))),
                                new Function.ValueExpression(new FieldReference<>("l_tax", Numeric.class, 2)),
                                Function.FunctionType.Add),
                        Function.FunctionType.Mul
                ), "charge");
        function1.addChild(function2);
        GroupBy groupBy =
                new GroupBy(
                        new KeyGroup(
                                new FieldReference("l_returnflag", Char.class),
                                new FieldReference("l_linestatus", Char.class)),
                        new Aggregation.Count(),
                        new Aggregation.Sum(new FieldReference("l_quantity", Numeric.class, 2)),
                        new Aggregation.Sum(new FieldReference("l_extendedprice", Numeric.class, 2)),
                        new Aggregation.Sum(new FieldReference("disc_price", Numeric.class, 4)),
                        new Aggregation.Sum(new FieldReference("charge", Numeric.class, 6))
                );
        function2.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);

        return sink;
    }


    public static LogicalOperator getExecution(String language) {
        switch (language) {
            case "rel":
                return relationalQueryTCPH1();
            case "python":
                return pythonQueryTCPH1();
            case "java":
                return javaQueryTCPH1();
            case "js":
                return javaScriptQueryTCPH1();
        }
        throw new RuntimeException("Language: " + language + " not suported in " + StringEquals.class.getClass().getName());
    }

}
