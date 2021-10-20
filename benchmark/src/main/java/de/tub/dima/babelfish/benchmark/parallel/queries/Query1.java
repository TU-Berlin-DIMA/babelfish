package de.tub.dima.babelfish.benchmark.parallel.queries;

import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.ParallelScan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.*;
import de.tub.dima.babelfish.ir.lqp.schema.FieldConstant;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;

public class Query1 {

    public static LogicalOperator relationalQueryTCPH18(int threads) {
        // scan lineitem and group by l_orderkey
        ParallelScan scanLineitems = new ParallelScan("table.lineitem", threads, 100_000);
        GroupBy groupByLineitem = new GroupBy(new KeyGroup(new FieldReference("l_orderkey", Int_32.class)),
                20_000_000,
                new Aggregation.Sum(new FieldReference<>("l_quantity", Numeric.class, 2)));
        scanLineitems.addChild(groupByLineitem);


        Selection selectionGroupBy = new Selection(
                new Predicate.GreaterThan(
                        new FieldReference<>("sum_0", Numeric.class, 2),
                        new FieldConstant<>(new EagerNumeric(30000, 2))
                )
        );
        groupByLineitem.addChild(selectionGroupBy);

        Sink sink = new Sink.MemorySink();
        selectionGroupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator relationalQueryTCPH1(int threads) {
        ParallelScan scan = new ParallelScan("table.lineitem", threads, 1_000_000);
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
                        new Aggregation.Sum(new FieldReference("disc_price", Numeric.class, 4)),
                        new Aggregation.Sum(new FieldReference("l_quantity", Numeric.class, 2)),
                        new Aggregation.Sum(new FieldReference("l_extendedprice", Numeric.class, 2)),
                        new Aggregation.Sum(new FieldReference("charge", Numeric.class, 6))
                );
        function2.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);

        return sink;
    }
}
