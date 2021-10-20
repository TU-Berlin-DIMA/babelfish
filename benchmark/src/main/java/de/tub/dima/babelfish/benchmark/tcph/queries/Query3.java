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
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class Query3 {

    static DynamicFilterFunction tcphfilterTypedUDF = new DynamicFilterFunction() {
        @Override
        public boolean filter(DynamicRecord record) {
            Text c_mktsegment = record.getValue("c_mktsegment");
            return c_mktsegment.equals(new StringText("BUILDING", 10));
        }
    };

    public static LogicalOperator relationalQuery3() {
        Scan scanCustomer = new Scan("table.customer");
        Selection selectionCustomer = new Selection(
                new Predicate.Equal(
                        new FieldReference<>("c_mktsegment", Text.class),
                        new FieldConstant<>(new StringText("BUILDING", 10))
                )
        );
        scanCustomer.addChild(selectionCustomer);
        Projection projectionCustomer = new Projection(new FieldReference("c_custkey", Int_32.class));
        selectionCustomer.addChild(projectionCustomer);

        Scan scanOrders = new Scan("table.orders");

        Selection selectionOrders = new Selection(
                new Predicate.LessThen(
                        new FieldReference<>("o_orderdate", Text.class),
                        new FieldConstant<>(new Date("1995-03-15"))
                )
        );
        scanOrders.addChild(selectionOrders);


        Join joinCustomer_Orders = new Join.EqualJoin<>(new FieldReference<>("c_custkey", Int_32.class),
                new FieldReference<>("o_custkey", Int_32.class));
        projectionCustomer.addChild(joinCustomer_Orders);
        selectionOrders.addChild(joinCustomer_Orders);

        Projection projectionCustomer_Orders = new Projection(
                new FieldReference("o_orderkey", Int_32.class),
                new FieldReference("o_orderdate", Date.class),
                new FieldReference("o_shippriority", Int_32.class));
        joinCustomer_Orders.addChild(projectionCustomer_Orders);

        Scan scanLineitems = new Scan("table.lineitem");
        Selection selectionLineitem = new Selection(
                new Predicate.GreaterThan(
                        new FieldReference<>("l_shipdate", Text.class),
                        new FieldConstant<>(new Date("1995-03-15"))
                )
        );
        scanLineitems.addChild(selectionLineitem);

        Join joinOrdersLineitem = new Join.EqualJoin<>(new FieldReference<>("o_orderkey", Int_32.class),
                new FieldReference<>("l_orderkey", Int_32.class));

        projectionCustomer_Orders.addChild(joinOrdersLineitem);
        selectionLineitem.addChild(joinOrdersLineitem);

        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class, 2)),
                        Function.FunctionType.Mul
                ), "revenue");
        joinOrdersLineitem.addChild(function);

        GroupBy groupBy = new GroupBy(new KeyGroup(
                new FieldReference("l_orderkey", Int_32.class),
                new FieldReference("o_orderdate", Date.class),
                new FieldReference("o_shippriority", Int_32.class)),
                new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 4)));
        function.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator pythonQuery3() {
        Scan scanCustomer = new Scan("table.customer");

        PythonOperator selectionCustomer = new PythonOperator(new PythonSelectionUDF(
                "lambda rec,ctx:  rec.c_mktsegment.equals(\"BUILDING\")"));
        scanCustomer.addChild(selectionCustomer);

        scanCustomer.addChild(selectionCustomer);
        Projection projectionCustomer = new Projection(new FieldReference("c_custkey", Int_32.class));
        selectionCustomer.addChild(projectionCustomer);

        Scan scanOrders = new Scan("table.orders");

        Selection selectionOrders = new Selection(
                new Predicate.LessThen(
                        new FieldReference<>("o_orderdate", Text.class),
                        new FieldConstant<>(new Date("1995-03-15"))
                )
        );
        scanOrders.addChild(selectionOrders);


        Join joinCustomer_Orders = new Join.EqualJoin<>(new FieldReference<>("c_custkey", Int_32.class),
                new FieldReference<>("o_custkey", Int_32.class));
        projectionCustomer.addChild(joinCustomer_Orders);
        selectionOrders.addChild(joinCustomer_Orders);

        Projection projectionCustomer_Orders = new Projection(
                new FieldReference("o_orderkey", Int_32.class),
                new FieldReference("o_orderdate", Date.class),
                new FieldReference("o_shippriority", Int_32.class));
        joinCustomer_Orders.addChild(projectionCustomer_Orders);

        Scan scanLineitems = new Scan("table.lineitem");
        Selection selectionLineitem = new Selection(
                new Predicate.GreaterThan(
                        new FieldReference<>("l_shipdate", Text.class),
                        new FieldConstant<>(new Date("1995-03-15"))
                )
        );
        scanLineitems.addChild(selectionLineitem);

        Join joinOrdersLineitem = new Join.EqualJoin<>(new FieldReference<>("o_orderkey", Int_32.class),
                new FieldReference<>("l_orderkey", Int_32.class));

        projectionCustomer_Orders.addChild(joinOrdersLineitem);
        selectionLineitem.addChild(joinOrdersLineitem);

        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class, 2)),
                        Function.FunctionType.Mul
                ), "revenue");
        joinOrdersLineitem.addChild(function);

        GroupBy groupBy = new GroupBy(new KeyGroup(
                new FieldReference("l_orderkey", Int_32.class),
                new FieldReference("o_orderdate", Date.class),
                new FieldReference("o_shippriority", Int_32.class)),
                new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 4)));
        function.addChild(groupBy);

        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator javaScriptQuery3() {
        Scan scanCustomer = new Scan("table.customer");

        JavaScriptOperator selectionCustomer = new JavaScriptOperator(new JavaScriptSelectionUDF("(record,ctx)=>{" +
                "return record.c_mktsegment.equals(\"BUILDING\")" +
                "}"));

        scanCustomer.addChild(selectionCustomer);
        Projection projectionCustomer = new Projection(new FieldReference("c_custkey", Int_32.class));
        selectionCustomer.addChild(projectionCustomer);

        Scan scanOrders = new Scan("table.orders");

        Selection selectionOrders = new Selection(
                new Predicate.LessThen(
                        new FieldReference<>("o_orderdate", Text.class),
                        new FieldConstant<>(new Date("1995-03-15"))
                )
        );
        scanOrders.addChild(selectionOrders);


        Join joinCustomer_Orders = new Join.EqualJoin<>(new FieldReference<>("c_custkey", Int_32.class),
                new FieldReference<>("o_custkey", Int_32.class));
        projectionCustomer.addChild(joinCustomer_Orders);
        selectionOrders.addChild(joinCustomer_Orders);

        Projection projectionCustomer_Orders = new Projection(
                new FieldReference("o_orderkey", Int_32.class),
                new FieldReference("o_orderdate", Date.class),
                new FieldReference("o_shippriority", Int_32.class));
        joinCustomer_Orders.addChild(projectionCustomer_Orders);

        Scan scanLineitems = new Scan("table.lineitem");
        Selection selectionLineitem = new Selection(
                new Predicate.GreaterThan(
                        new FieldReference<>("l_shipdate", Text.class),
                        new FieldConstant<>(new Date("1995-03-15"))
                )
        );
        scanLineitems.addChild(selectionLineitem);

        Join joinOrdersLineitem = new Join.EqualJoin<>(new FieldReference<>("o_orderkey", Int_32.class),
                new FieldReference<>("l_orderkey", Int_32.class));

        projectionCustomer_Orders.addChild(joinOrdersLineitem);
        selectionLineitem.addChild(joinOrdersLineitem);

        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class, 2)),
                        Function.FunctionType.Mul
                ), "revenue");
        joinOrdersLineitem.addChild(function);

        GroupBy groupBy = new GroupBy(new KeyGroup(
                new FieldReference("l_orderkey", Int_32.class),
                new FieldReference("o_orderdate", Date.class),
                new FieldReference("o_shippriority", Int_32.class)),
                new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 4)));
        function.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator javaQuery3() {
        Scan scanCustomer = new Scan("table.customer");

        UDFOperator selectionCustomer = new JavaDynamicUDFOperator(tcphfilterTypedUDF);

        scanCustomer.addChild(selectionCustomer);
        Projection projectionCustomer = new Projection(new FieldReference("c_custkey", Int_32.class));
        selectionCustomer.addChild(projectionCustomer);

        Scan scanOrders = new Scan("table.orders");

        Selection selectionOrders = new Selection(
                new Predicate.LessThen(
                        new FieldReference<>("o_orderdate", Text.class),
                        new FieldConstant<>(new Date("1995-03-15"))
                )
        );
        scanOrders.addChild(selectionOrders);


        Join joinCustomer_Orders = new Join.EqualJoin<>(new FieldReference<>("c_custkey", Int_32.class),
                new FieldReference<>("o_custkey", Int_32.class));
        projectionCustomer.addChild(joinCustomer_Orders);
        selectionOrders.addChild(joinCustomer_Orders);

        Projection projectionCustomer_Orders = new Projection(
                new FieldReference("o_orderkey", Int_32.class),
                new FieldReference("o_orderdate", Date.class),
                new FieldReference("o_shippriority", Int_32.class));
        joinCustomer_Orders.addChild(projectionCustomer_Orders);

        Scan scanLineitems = new Scan("table.lineitem");
        Selection selectionLineitem = new Selection(
                new Predicate.GreaterThan(
                        new FieldReference<>("l_shipdate", Text.class),
                        new FieldConstant<>(new Date("1995-03-15"))
                )
        );
        scanLineitems.addChild(selectionLineitem);

        Join joinOrdersLineitem = new Join.EqualJoin<>(new FieldReference<>("o_orderkey", Int_32.class),
                new FieldReference<>("l_orderkey", Int_32.class));

        projectionCustomer_Orders.addChild(joinOrdersLineitem);
        selectionLineitem.addChild(joinOrdersLineitem);

        Function<?> function = new Function<>(
                new Function.BinaryExpression(
                        new Function.ValueExpression(new FieldReference<>("l_extendedprice", Numeric.class, 2)),
                        new Function.ValueExpression(new FieldReference<>("l_discount", Numeric.class, 2)),
                        Function.FunctionType.Mul
                ), "revenue");
        joinOrdersLineitem.addChild(function);

        GroupBy groupBy = new GroupBy(new KeyGroup(
                new FieldReference("l_orderkey", Int_32.class),
                new FieldReference("o_orderdate", Date.class),
                new FieldReference("o_shippriority", Int_32.class)),
                new Aggregation.Sum(new FieldReference<>("revenue", Numeric.class, 4)));
        function.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }


    public static LogicalOperator getExecution(String language) {
        switch (language) {
            case "rel":
                return relationalQuery3();
            case "python":
                return pythonQuery3();
            case "java":
                return javaQuery3();
            case "js":
                return javaScriptQuery3();
        }
        throw new RuntimeException("Language: " + language + " not suported in " + StringEquals.class.getClass().getName());
    }
}
