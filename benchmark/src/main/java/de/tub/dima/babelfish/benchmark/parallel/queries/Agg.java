package de.tub.dima.babelfish.benchmark.parallel.queries;

import de.tub.dima.babelfish.ir.lqp.ParallelScan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.Aggregation;
import de.tub.dima.babelfish.ir.lqp.relational.GroupBy;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;

import java.io.Serializable;

public class Agg implements Serializable {


    public static Sink add(int threads) {

        ParallelScan scan = new ParallelScan("table.lineitem", threads, 10000);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("l_discount", Numeric.class, 2)));
        scan.addChild(groupBy);
        Sink sink = new Sink.PrintSink();
        groupBy.addChild(sink);
        return sink;
    }

    public static Sink min(int threads) {

        ParallelScan scan = new ParallelScan("table.lineitem", threads, 10000);

        GroupBy groupBy = new GroupBy(new Aggregation.Min(new FieldReference<>("l_discount", Numeric.class, 2)));
        scan.addChild(groupBy);
        Sink sink = new Sink.PrintSink();
        groupBy.addChild(sink);
        return sink;
    }


}
