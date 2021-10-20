package de.tub.dima.babelfish.benchmark.datasources.queries;

import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.*;
import de.tub.dima.babelfish.ir.lqp.schema.FieldConstant;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;

import java.io.IOException;

public class DataSourceQuery6 {


    public static Sink relationalQueryTCPH6_csv(LogicalOperator scan) throws IOException, SchemaExtractionException, InterruptedException {
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
                                                        new FieldConstant<>(new EagerNumeric(24, 0))),
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

    public static Sink relationalQueryTCPH6_Arrow(LogicalOperator scan) throws IOException, SchemaExtractionException, InterruptedException {

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
                                                        new FieldConstant<>(new EagerNumeric(2400, 2))),
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
}
