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
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptUDF;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonUDF;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;

public class SSBQuery12 {

    static DynamicFilterFunction javafilterTypedUDF = new DynamicFilterFunction() {
        @Override
        public boolean filter(DynamicRecord record) {
            Int_32 lo_quantity = record.getValue("lo_quantity");
            Numeric lo_discount = record.getValue("lo_discount");
            return lo_quantity.asInt() >= 26 && lo_quantity.asInt() <= 35 && lo_discount.getValue() >= 400 && lo_discount.getValue() <= 600;
        }
    };

    public static LogicalOperator relationalQuery() {
        Scan scan1 = new Scan("table.sbb_date");
        Projection projection1 = new Projection(new FieldReference("d_yearmonthnum", Int_32.class), new FieldReference("d_datekey", Int_32.class));
        scan1.addChild(projection1);

        Selection selection1 = new Selection(
                new Predicate.Equal<>(
                        new FieldReference<>("d_yearmonthnum", Int_32.class),
                        new FieldConstant<>(new Eager_Int_32(199401)))

        );
        projection1.addChild(selection1);

        Scan scan2 = new Scan("table.sbb_lineorder");

        Selection selection2 = new Selection(
                new Predicate.And(
                        new Predicate.And(
                                new Predicate.GreaterEquals<>(
                                        new FieldReference<>("lo_quantity", Int_32.class),
                                        new FieldConstant<>(new Eager_Int_32(26))),
                                new Predicate.LessEquals<>(
                                        new FieldReference<>("lo_quantity", Int_32.class),
                                        new FieldConstant<>(new Eager_Int_32(35)))),
                        new Predicate.And(
                                new Predicate.GreaterEquals<>(
                                        new FieldReference<>("lo_discount", Numeric.class, 2),
                                        new FieldConstant<>(new EagerNumeric(400, 2))),
                                new Predicate.LessEquals<>(
                                        new FieldReference<>("lo_discount", Numeric.class, 2),
                                        new FieldConstant<>(new EagerNumeric(600, 2))))));
        scan2.addChild(selection2);

        Join join = new Join.EqualJoin<>(new FieldReference<>("d_datekey", Int_32.class), new FieldReference<>("lo_orderdate", Int_32.class));
        selection1.addChild(join);
        selection2.addChild(join);


        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("lo_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("lo_discount", Numeric.class, 2)),
                        Function.FunctionType.Mul
                ), "revenue");
        join.addChild(function);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 4)));
        function.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator pythonQuery() {
        Scan scan1 = new Scan("table.sbb_date");
        Projection projection1 = new Projection(new FieldReference("d_yearmonthnum", Int_32.class), new FieldReference("d_datekey", Int_32.class));
        scan1.addChild(projection1);
        Selection selection1 = new Selection(
                new Predicate.Equal<>(
                        new FieldReference<>("d_yearmonthnum", Int_32.class),
                        new FieldConstant<>(new Eager_Int_32(199401)))

        );
        projection1.addChild(selection1);

        Scan scan2 = new Scan("table.sbb_lineorder");


        PythonOperator selection2 = new PythonOperator(new PythonUDF(
                "lambda rec,ctx: (rec.lo_quantity>=26) and (rec.lo_quantity<=35) and (rec.lo_discount >= 4) and (rec.lo_discount<=6)"));
        scan2.addChild(selection2);

        Join join = new Join.EqualJoin<>(new FieldReference<>("d_datekey", Int_32.class), new FieldReference<>("lo_orderdate", Int_32.class));
        selection1.addChild(join);
        selection2.addChild(join);


        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("lo_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("lo_discount", Numeric.class, 2)),
                        Function.FunctionType.Mul
                ), "revenue");
        join.addChild(function);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 4)));
        function.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator javaScriptQuery() {
        Scan scan1 = new Scan("table.sbb_date");
        Projection projection1 = new Projection(new FieldReference("d_yearmonthnum", Int_32.class), new FieldReference("d_datekey", Int_32.class));
        scan1.addChild(projection1);
        Selection selection1 = new Selection(
                new Predicate.Equal<>(
                        new FieldReference<>("d_yearmonthnum", Int_32.class),
                        new FieldConstant<>(new Eager_Int_32(199401)))

        );
        projection1.addChild(selection1);

        Scan scan2 = new Scan("table.sbb_lineorder");

        JavaScriptOperator selection2 = new JavaScriptOperator(new JavaScriptUDF("(record,ctx)=>{" +
                "return (record.lo_quantity>=26) &&  (record.lo_quantity<=35) && (record.lo_discount >= 4) && (record.lo_discount <= 6);" +
                "}"));
        scan2.addChild(selection2);

        Join join = new Join.EqualJoin<>(new FieldReference<>("d_datekey", Int_32.class), new FieldReference<>("lo_orderdate", Int_32.class));
        selection1.addChild(join);
        selection2.addChild(join);


        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("lo_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("lo_discount", Numeric.class, 2)),
                        Function.FunctionType.Mul
                ), "revenue");
        join.addChild(function);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 4)));
        function.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator javaQuery() {
        Scan scan1 = new Scan("table.sbb_date");
        Projection projection1 = new Projection(new FieldReference("d_yearmonthnum", Int_32.class), new FieldReference("d_datekey", Int_32.class));
        scan1.addChild(projection1);
        Selection selection1 = new Selection(
                new Predicate.Equal<>(
                        new FieldReference<>("d_yearmonthnum", Int_32.class),
                        new FieldConstant<>(new Eager_Int_32(199401)))

        );
        projection1.addChild(selection1);

        Scan scan2 = new Scan("table.sbb_lineorder");

        UDFOperator selection2 = new JavaDynamicUDFOperator(javafilterTypedUDF);
        scan2.addChild(selection2);

        Join join = new Join.EqualJoin<>(new FieldReference<>("d_datekey", Int_32.class), new FieldReference<>("lo_orderdate", Int_32.class));
        selection1.addChild(join);
        selection2.addChild(join);


        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("lo_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("lo_discount", Numeric.class, 2)),
                        Function.FunctionType.Mul
                ), "revenue");
        join.addChild(function);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 4)));
        function.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }


    public static LogicalOperator getExecution(String language) {
        switch (language) {
            case "rel":
                return relationalQuery();
            case "python":
                return pythonQuery();
            case "java":
                return javaQuery();
            case "js":
                return javaScriptQuery();
        }
        throw new RuntimeException("Language: " + language + " not supported in " + StringEquals.class.getClass().getName());
    }
}
