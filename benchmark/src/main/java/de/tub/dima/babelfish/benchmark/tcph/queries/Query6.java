package de.tub.dima.babelfish.benchmark.tcph.queries;

import de.tub.dima.babelfish.benchmark.string.queries.StringEquals;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
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
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;

import java.io.Serializable;

public class Query6 implements Serializable {

    static DynamicFilterFunction tcphfilterTypedUDF = new DynamicFilterFunction() {
        @Override
        public boolean filter(DynamicRecord record) {
            LazyDate l_shipdate = record.getValue("l_shipdate");
            EagerNumeric l_discount = record.getValue("l_discount");
            EagerNumeric l_quantity = record.getValue("l_quantity");
            return l_shipdate.after("1994-01-01") & l_shipdate.before("1995-01-01") && l_discount.getValue() >= 5 & l_discount.getValue() <= 7 & l_quantity.getValue() < 2400;
        }
    };

    public static LogicalOperator javaScriptTCPH6Query() {

        Scan scan = new Scan("table.lineitem");
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptSelectionUDF("(record,ctx)=>{" +
                "return (record.l_shipdate.after(\"1994-01-01\") & record.l_shipdate.before(\"1995-01-01\") && record.l_discount >= 0.05 & record.l_discount <= 0.07 & record.l_quantity < 24)" +
                "}"));

        scan.addChild(selection);

        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class, 2)),
                        Function.FunctionType.Mul
                ), "revenue");
        selection.addChild(function);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 2)));
        function.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);

        return sink;
    }

    /*
        static TypedFilterFunction<Lineitem> tcphfilterTypedUDF = new TypedFilterFunction<Lineitem>() {
            @Override
            public boolean filter(Lineitem record) {
                return record.l_shipdate.after("1994-01-01") &
                        record.l_shipdate.before("1995-01-01") &&
                        record.l_discount.getValue() > 5 &
                        record.l_discount.getValue() < 7 &
                        record.l_quantity.getValue() < 24;
            }
        };
    */
    public static LogicalOperator javaTcph6TypedUDFQuery() {
        Scan scan = new Scan("table.lineitem");
        UDFOperator selection = new JavaDynamicUDFOperator(tcphfilterTypedUDF);
        scan.addChild(selection);

        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class, 2)),
                        Function.FunctionType.Mul
                ), "revenue");
        selection.addChild(function);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 2)));
        function.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);

        return sink;
    }


    public static LogicalOperator pythonTCPHUDFQuery() {
        RuntimeConfiguration.ELIMINATE_EMPTY_IF = true;
        Scan scan = new Scan("table.lineitem");
        PythonOperator selection = new PythonOperator(new PythonSelectionUDF(
                "lambda rec,ctx:  rec.l_shipdate.after(\"1994-01-01\") and rec.l_shipdate.before(\"1995-01-01\") and (rec.l_discount >= 0.05) and (rec.l_discount <= 0.07) and (rec.l_quantity < 24)"));
        scan.addChild(selection);

        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class, 2)),
                        Function.FunctionType.Mul
                ), "revenue");
        selection.addChild(function);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 2)));
        function.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);

        return sink;
    }

    public static LogicalOperator relationalQueryTCPH6() {
        Scan scan = new Scan("table.lineitem");
        Selection selection = new Selection(
                new Predicate.And(
                        new Predicate.GreaterEquals<>(
                                new FieldReference<>("l_shipdate", Date.class),
                                new FieldConstant<>(new Date("1994-01-01"))),

                        new Predicate.And(
                                new Predicate.LessThen<>(
                                        new FieldReference<>("l_shipdate", Date.class),
                                        new FieldConstant<>(new Date("1995-01-01"))),
                                new Predicate.And(
                                        new Predicate.LessEquals<>(
                                                new FieldReference<>("l_discount", Numeric.class, 2),
                                                new FieldConstant<>(new EagerNumeric(7, 2))),
                                        new Predicate.And(
                                                new Predicate.LessThen<>(
                                                        new FieldReference<>("l_quantity", Numeric.class, 2),
                                                        new FieldConstant<>(new EagerNumeric(2400, 0))),
                                                new Predicate.GreaterEquals<>(
                                                        new FieldReference<>("l_discount", Numeric.class, 2),
                                                        new FieldConstant<>(new EagerNumeric(5, 2)))
                                        )
                                )
                        )
                ));
        scan.addChild(selection);

        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class, 2)),
                        Function.FunctionType.Mul
                ), "revenue");
        selection.addChild(function);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 2)));
        function.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }


    public static LogicalOperator getExecution(String language) {
        switch (language) {
            case "rel":
                return relationalQueryTCPH6();
            case "python":
                return pythonTCPHUDFQuery();
            case "java":
                return javaTcph6TypedUDFQuery();
            case "js":
                return javaScriptTCPH6Query();

        }
        throw new RuntimeException("Language: " + language + " not suported in " + StringEquals.class.getClass().getName());
    }


}
