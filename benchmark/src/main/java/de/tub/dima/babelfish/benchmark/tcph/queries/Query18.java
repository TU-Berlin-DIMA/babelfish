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
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class Query18 {

    static DynamicFilterFunction tcphfilterTypedUDF = new DynamicFilterFunction() {
        @Override
        public boolean filter(DynamicRecord record) {
            Numeric sum_0 = record.getValue("sum_0");
            return sum_0.getValue() > 30000;
        }
    };

    public static LogicalOperator relationalQuery18() {

        // scan lineitem and group by l_orderkey
        Scan scanLineitems = new Scan("table.lineitem");
        GroupBy groupByLineitem = new GroupBy(new KeyGroup(new FieldReference("l_orderkey", Int_32.class)),
                1500000,
                new Aggregation.Sum(new FieldReference<>("l_quantity", Numeric.class, 2)));
        scanLineitems.addChild(groupByLineitem);


        Selection selectionGroupBy = new Selection(
                new Predicate.GreaterThan(
                        new FieldReference<>("sum_0", Numeric.class, 2),
                        new FieldConstant<>(new EagerNumeric(30000, 2))
                )
        );
        groupByLineitem.addChild(selectionGroupBy);
        Projection projectGroupBy = new Projection(
                new FieldReference("l_orderkey", Int_32.class));
        selectionGroupBy.addChild(projectGroupBy);

        Scan scanOrders = new Scan("table.orders");


        // join with ht1
        Join joinGroupBy_Orders = new Join.EqualJoin<>(57,
                new FieldReference<>("l_orderkey", Int_32.class),
                new FieldReference<>("o_orderkey", Int_32.class));
        projectGroupBy.addChild(joinGroupBy_Orders);
        scanOrders.addChild(joinGroupBy_Orders);


        Scan scanCustomer = new Scan("table.customer");
        Projection projectionCustomer = new Projection(
                new FieldReference("c_custkey", Int_32.class),
                new FieldReference("c_name", Text.class, 25));
        scanCustomer.addChild(projectionCustomer);

        // join with ht2
        Join joinCustomer_Orders = new Join.EqualJoin<>(150000,
                new FieldReference<>("c_custkey", Int_32.class),
                new FieldReference<>("o_custkey", Int_32.class));

        projectionCustomer.addChild(joinCustomer_Orders);
        joinGroupBy_Orders.addChild(joinCustomer_Orders);

        Projection projectionCustomer_Orders = new Projection(
                new FieldReference("o_orderkey", Int_32.class),
                new FieldReference("c_custkey", Int_32.class),
                new FieldReference("o_orderdate", Date.class),
                new FieldReference("o_totalprice", Float_32.class),
                new FieldReference("c_name", Text.class, 25)
        );
        joinCustomer_Orders.addChild(projectionCustomer_Orders);


        // scan lineitem and join
        Scan scanLineitems2 = new Scan("table.lineitem");

        // join with ht3
        Join joinLineitemOrder = new Join.EqualJoin<>(57,
                new FieldReference<>("o_orderkey", Int_32.class),
                new FieldReference<>("l_orderkey", Int_32.class));
        projectionCustomer_Orders.addChild(joinLineitemOrder);
        scanLineitems2.addChild(joinLineitemOrder);

        GroupBy groupBy = new GroupBy(
                new KeyGroup(
                        new FieldReference("c_name", Text.class, 25),
                        new FieldReference("c_custkey", Int_32.class),
                        new FieldReference("o_orderkey", Int_32.class),
                        new FieldReference("o_orderdate", Date.class),
                        new FieldReference("o_totalprice", Float_32.class)
                ),
                5700 ,
                new Aggregation.Sum(new FieldReference<>("l_quantity", Numeric.class, 2)));
        joinLineitemOrder.addChild(groupBy);

        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;

    }

    public static LogicalOperator pythonQuery18() {

        // scan lineitem and group by l_orderkey
        Scan scanLineitems = new Scan("table.lineitem");
        GroupBy groupByLineitem = new GroupBy(new KeyGroup(new FieldReference("l_orderkey", Int_32.class)),
                1500000,
                new Aggregation.Sum(new FieldReference<>("l_quantity", Numeric.class, 2)));
        scanLineitems.addChild(groupByLineitem);

        PythonOperator selectionGroupBy = new PythonOperator(new PythonSelectionUDF(
                "lambda rec,ctx:  rec.sum_0 > 300"));
        groupByLineitem.addChild(selectionGroupBy);
        Projection projectGroupBy = new Projection(
                new FieldReference("l_orderkey", Int_32.class));
        selectionGroupBy.addChild(projectGroupBy);


        Scan scanOrders = new Scan("table.orders");


        // join with ht1
        Join joinGroupBy_Orders = new Join.EqualJoin<>(57,
                new FieldReference<>("l_orderkey", Int_32.class),
                new FieldReference<>("o_orderkey", Int_32.class));
        projectGroupBy.addChild(joinGroupBy_Orders);
        scanOrders.addChild(joinGroupBy_Orders);

        Scan scanCustomer = new Scan("table.customer");
        Projection projectionCustomer = new Projection(
                new FieldReference("c_custkey", Int_32.class),
                new FieldReference("c_name", Text.class, 25));
        scanCustomer.addChild(projectionCustomer);

        // join with ht2
        Join joinCustomer_Orders = new Join.EqualJoin<>(150000,
                new FieldReference<>("c_custkey", Int_32.class),
                new FieldReference<>("o_custkey", Int_32.class));
        projectionCustomer.addChild(joinCustomer_Orders);
        joinGroupBy_Orders.addChild(joinCustomer_Orders);

        Projection projectionCustomer_Orders = new Projection(
                new FieldReference("o_orderkey", Int_32.class),
                new FieldReference("c_custkey", Int_32.class),
                new FieldReference("o_orderdate", Date.class),
                new FieldReference("o_totalprice", Float_32.class),
                new FieldReference("c_name", Text.class, 25)
        );
        joinCustomer_Orders.addChild(projectionCustomer_Orders);


        // scan lineitem and join
        Scan scanLineitems2 = new Scan("table.lineitem");

        // join with ht3
        Join joinLineitemOrder = new Join.EqualJoin<>(57,
                new FieldReference<>("o_orderkey", Int_32.class),
                new FieldReference<>("l_orderkey", Int_32.class));
        projectionCustomer_Orders.addChild(joinLineitemOrder);
        scanLineitems2.addChild(joinLineitemOrder);

        GroupBy groupBy = new GroupBy(
                new KeyGroup(
                        new FieldReference("c_name", Text.class, 25),
                        new FieldReference("c_custkey", Int_32.class),
                        new FieldReference("o_orderkey", Int_32.class),
                        new FieldReference("o_orderdate", Date.class),
                        new FieldReference("o_totalprice", Float_32.class)
                ),
                57,
                new Aggregation.Sum(new FieldReference<>("l_quantity", Numeric.class, 2)));
        joinLineitemOrder.addChild(groupBy);

        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator javaScriptQuery18() {

        // scan lineitem and group by l_orderkey
        Scan scanLineitems = new Scan("table.lineitem");
        GroupBy groupByLineitem = new GroupBy(new KeyGroup(new FieldReference("l_orderkey", Int_32.class)),
                1500000,
                new Aggregation.Sum(new FieldReference<>("l_quantity", Numeric.class, 2)));
        scanLineitems.addChild(groupByLineitem);

        JavaScriptOperator selectionGroupBy = new JavaScriptOperator(new JavaScriptSelectionUDF("(record,ctx)=>{" +
                "return record.sum_0 > 300;" +
                " }"));

        groupByLineitem.addChild(selectionGroupBy);
        Projection projectGroupBy = new Projection(
                new FieldReference("l_orderkey", Int_32.class));
        selectionGroupBy.addChild(projectGroupBy);


        Scan scanOrders = new Scan("table.orders");


        // join with ht1
        Join joinGroupBy_Orders = new Join.EqualJoin<>(57,
                new FieldReference<>("l_orderkey", Int_32.class),
                new FieldReference<>("o_orderkey", Int_32.class));
        projectGroupBy.addChild(joinGroupBy_Orders);
        scanOrders.addChild(joinGroupBy_Orders);

        Scan scanCustomer = new Scan("table.customer");
        Projection projectionCustomer = new Projection(
                new FieldReference("c_custkey", Int_32.class),
                new FieldReference("c_name", Text.class, 25));
        scanCustomer.addChild(projectionCustomer);

        // join with ht2
        Join joinCustomer_Orders = new Join.EqualJoin<>(150000,
                new FieldReference<>("c_custkey", Int_32.class),
                new FieldReference<>("o_custkey", Int_32.class));
        projectionCustomer.addChild(joinCustomer_Orders);
        joinGroupBy_Orders.addChild(joinCustomer_Orders);

        Projection projectionCustomer_Orders = new Projection(
                new FieldReference("o_orderkey", Int_32.class),
                new FieldReference("c_custkey", Int_32.class),
                new FieldReference("o_orderdate", Date.class),
                new FieldReference("o_totalprice", Float_32.class),
                new FieldReference("c_name", Text.class, 25)
        );
        joinCustomer_Orders.addChild(projectionCustomer_Orders);


        // scan lineitem and join
        Scan scanLineitems2 = new Scan("table.lineitem");

        // join with ht3
        Join joinLineitemOrder = new Join.EqualJoin<>(57,
                new FieldReference<>("o_orderkey", Int_32.class),
                new FieldReference<>("l_orderkey", Int_32.class));
        projectionCustomer_Orders.addChild(joinLineitemOrder);
        scanLineitems2.addChild(joinLineitemOrder);

        GroupBy groupBy = new GroupBy(
                new KeyGroup(
                        new FieldReference("c_name", Text.class, 25),
                        new FieldReference("c_custkey", Int_32.class),
                        new FieldReference("o_orderkey", Int_32.class),
                        new FieldReference("o_orderdate", Date.class),
                        new FieldReference("o_totalprice", Float_32.class)
                ),
                57,
                new Aggregation.Sum(new FieldReference<>("l_quantity", Numeric.class, 2)));
        joinLineitemOrder.addChild(groupBy);

        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator javaQuery18() {

        // scan lineitem and group by l_orderkey
        Scan scanLineitems = new Scan("table.lineitem");
        GroupBy groupByLineitem = new GroupBy(new KeyGroup(new FieldReference("l_orderkey", Int_32.class)),
                1500000,
                new Aggregation.Sum(new FieldReference<>("l_quantity", Numeric.class, 2)));
        scanLineitems.addChild(groupByLineitem);

        UDFOperator selectionGroupBy = new JavaDynamicUDFOperator(tcphfilterTypedUDF);

        groupByLineitem.addChild(selectionGroupBy);
        Projection projectGroupBy = new Projection(
                new FieldReference("l_orderkey", Int_32.class));
        selectionGroupBy.addChild(projectGroupBy);


        Scan scanOrders = new Scan("table.orders");


        // join with ht1
        Join joinGroupBy_Orders = new Join.EqualJoin<>(57,
                new FieldReference<>("l_orderkey", Int_32.class),
                new FieldReference<>("o_orderkey", Int_32.class));
        projectGroupBy.addChild(joinGroupBy_Orders);
        scanOrders.addChild(joinGroupBy_Orders);

        Scan scanCustomer = new Scan("table.customer");
        Projection projectionCustomer = new Projection(
                new FieldReference("c_custkey", Int_32.class),
                new FieldReference("c_name", Text.class, 25));
        scanCustomer.addChild(projectionCustomer);

        // join with ht2
        Join joinCustomer_Orders = new Join.EqualJoin<>(150000,
                new FieldReference<>("c_custkey", Int_32.class),
                new FieldReference<>("o_custkey", Int_32.class));
        projectionCustomer.addChild(joinCustomer_Orders);
        joinGroupBy_Orders.addChild(joinCustomer_Orders);

        Projection projectionCustomer_Orders = new Projection(
                new FieldReference("o_orderkey", Int_32.class),
                new FieldReference("c_custkey", Int_32.class),
                new FieldReference("o_orderdate", Date.class),
                new FieldReference("o_totalprice", Float_32.class),
                new FieldReference("c_name", Text.class, 25)
        );
        joinCustomer_Orders.addChild(projectionCustomer_Orders);


        // scan lineitem and join
        Scan scanLineitems2 = new Scan("table.lineitem");

        // join with ht3
        Join joinLineitemOrder = new Join.EqualJoin<>(57,
                new FieldReference<>("o_orderkey", Int_32.class),
                new FieldReference<>("l_orderkey", Int_32.class));
        projectionCustomer_Orders.addChild(joinLineitemOrder);
        scanLineitems2.addChild(joinLineitemOrder);

        GroupBy groupBy = new GroupBy(
                new KeyGroup(
                        new FieldReference("c_name", Text.class, 25),
                        new FieldReference("c_custkey", Int_32.class),
                        new FieldReference("o_orderkey", Int_32.class),
                        new FieldReference("o_orderdate", Date.class),
                        new FieldReference("o_totalprice", Float_32.class)
                ),
                57,
                new Aggregation.Sum(new FieldReference<>("l_quantity", Numeric.class, 2)));
        joinLineitemOrder.addChild(groupBy);

        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator getExecution(String language) {
        switch (language) {
            case "rel":
                return relationalQuery18();
            case "python":
                return pythonQuery18();
            case "java":
                return javaQuery18();
            case "js":
                return javaScriptQuery18();
        }
        throw new RuntimeException("Language: " + language + " not supported in " + StringEquals.class.getClass().getName());
    }
}
