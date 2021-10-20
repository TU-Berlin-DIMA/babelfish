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
import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class SSBQuery41 {

    static DynamicFilterFunction javafilterTypedUDF = new DynamicFilterFunction() {
        @Override
        public boolean filter(DynamicRecord record) {
            AbstractRope p_mfgr = record.getValue("p_mfgr");
            return p_mfgr.equals(new StringText("MFGR#1", 6)) || p_mfgr.equals(new StringText("MFGR#2", 6));
        }
    };

    public static LogicalOperator relationalQuery() {
        Scan scanDate = new Scan("table.sbb_date");

        Projection projectDate = new Projection(new FieldReference("d_year", Int_32.class),
                new FieldReference("d_datekey", Int_32.class));
        scanDate.addChild(projectDate);

        Scan scanPart = new Scan("table.sbb_part");
        Selection selectionPart = new Selection(
                new Predicate.Or(
                        new Predicate.Equal<>(
                                new FieldReference<>("p_mfgr", Text.class),
                                new FieldConstant<>(new StringText("MFGR#1", 6))
                        ),
                        new Predicate.Equal<>(
                                new FieldReference<>("p_mfgr", Text.class),
                                new FieldConstant<>(new StringText("MFGR#2", 6))
                        ))
        );
        scanPart.addChild(selectionPart);
        Projection partProjecton = new Projection(new FieldReference("p_partkey", Int_32.class));
        selectionPart.addChild(partProjecton);


        Scan scanCustomer = new Scan("table.sbb_customer");

        Selection selectionCustomer = new Selection(
                new Predicate.Equal<>(
                        new FieldReference<>("c_region", Text.class),
                        new FieldConstant<>(new StringText("AMERICA", 12))
                )
        );
        scanCustomer.addChild(selectionCustomer);
        Projection customerProjecton = new Projection(new FieldReference("c_custkey", Int_32.class), new FieldReference("c_nation", Text.class, 15));
        selectionCustomer.addChild(customerProjecton);

        Scan scanSupplier = new Scan("table.sbb_supplier");

        Selection selectionSupplier = new Selection(
                new Predicate.Equal<>(
                        new FieldReference<>("s_region", Text.class),
                        new FieldConstant<>(new StringText("AMERICA", 12))
                )
        );
        scanSupplier.addChild(selectionSupplier);
        Projection projectSupplier = new Projection(new FieldReference("s_suppkey", Int_32.class));
        selectionSupplier.addChild(projectSupplier);


        Scan scan4 = new Scan("table.sbb_lineorder");

        Join join1 = new Join.EqualJoin<>(378, new FieldReference<>("s_suppkey", Int_32.class), new FieldReference<>("lo_suppkey", Int_32.class));
        projectSupplier.addChild(join1);
        scan4.addChild(join1);

        Join join2 = new Join.EqualJoin<>(5992, new FieldReference<>("c_custkey", Int_32.class), new FieldReference<>("lo_custkey", Int_32.class));
        customerProjecton.addChild(join2);
        join1.addChild(join2);

        Join join3 = new Join.EqualJoin<>(80045, new FieldReference<>("p_partkey", Int_32.class), new FieldReference<>("lo_partkey", Int_32.class));
        partProjecton.addChild(join3);
        join2.addChild(join3);

        Join join4 = new Join.EqualJoin<>(2556, new FieldReference<>("d_datekey", Int_32.class), new FieldReference<>("lo_orderdate", Int_32.class));
        projectDate.addChild(join4);
        join3.addChild(join4);


        GroupBy groupBy = new GroupBy(new KeyGroup(
                new FieldReference("d_year", Int_32.class),
                new FieldReference("c_nation", Text.class, 15)),
                new Aggregation.Sum(new FieldReference<>("lo_revenue", Numeric.class, 2)));
        join4.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        join4.addChild(sink);
        return sink;
    }

    public static LogicalOperator pythonQuery() {
        Scan scanDate = new Scan("table.sbb_date");

        Projection projectDate = new Projection(new FieldReference("d_year", Int_32.class),
                new FieldReference("d_datekey", Int_32.class));
        scanDate.addChild(projectDate);

        Scan scanPart = new Scan("table.sbb_part");

        PythonOperator selectionPart = new PythonOperator(new PythonUDF(
                "lambda rec,ctx: (rec.p_mfgr.equals(\"MFGR#1\") or rec.p_mfgr.equals(\"MFGR#2\"))"));

        scanPart.addChild(selectionPart);
        Projection partProjecton = new Projection(new FieldReference("p_partkey", Int_32.class));
        selectionPart.addChild(partProjecton);


        Scan scanCustomer = new Scan("table.sbb_customer");

        Selection selectionCustomer = new Selection(
                new Predicate.Equal<>(
                        new FieldReference<>("c_region", Text.class),
                        new FieldConstant<>(new StringText("AMERICA", 12))
                )
        );
        scanCustomer.addChild(selectionCustomer);
        Projection customerProjecton = new Projection(new FieldReference("c_custkey", Int_32.class), new FieldReference("c_nation", Text.class, 15));
        selectionCustomer.addChild(customerProjecton);


        Scan scanSupplier = new Scan("table.sbb_supplier");

        Selection selectionSupplier = new Selection(
                new Predicate.Equal<>(
                        new FieldReference<>("s_region", Text.class),
                        new FieldConstant<>(new StringText("AMERICA", 12))
                )
        );
        scanSupplier.addChild(selectionSupplier);
        Projection projectSupplier = new Projection(new FieldReference("s_suppkey", Int_32.class));
        selectionSupplier.addChild(projectSupplier);


        Scan scan4 = new Scan("table.sbb_lineorder");

        Join join1 = new Join.EqualJoin<>(378, new FieldReference<>("s_suppkey", Int_32.class), new FieldReference<>("lo_suppkey", Int_32.class));
        projectSupplier.addChild(join1);
        scan4.addChild(join1);

        Join join2 = new Join.EqualJoin<>(5992, new FieldReference<>("c_custkey", Int_32.class), new FieldReference<>("lo_custkey", Int_32.class));
        customerProjecton.addChild(join2);
        join1.addChild(join2);

        Join join3 = new Join.EqualJoin<>(80045, new FieldReference<>("p_partkey", Int_32.class), new FieldReference<>("lo_partkey", Int_32.class));
        partProjecton.addChild(join3);
        join2.addChild(join3);

        Join join4 = new Join.EqualJoin<>(2556, new FieldReference<>("d_datekey", Int_32.class), new FieldReference<>("lo_orderdate", Int_32.class));
        projectDate.addChild(join4);
        join3.addChild(join4);


        GroupBy groupBy = new GroupBy(new KeyGroup(
                new FieldReference("d_year", Int_32.class),
                new FieldReference("c_nation", Text.class, 15)),
                new Aggregation.Sum(new FieldReference<>("lo_revenue", Numeric.class, 2)));
        join4.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        join4.addChild(sink);
        return sink;
    }

    public static LogicalOperator javaScriptQuery() {
        Scan scanDate = new Scan("table.sbb_date");

        Projection projectDate = new Projection(new FieldReference("d_year", Int_32.class),
                new FieldReference("d_datekey", Int_32.class));
        scanDate.addChild(projectDate);

        Scan scanPart = new Scan("table.sbb_part");

        JavaScriptOperator selectionPart = new JavaScriptOperator(new JavaScriptUDF("(record,ctx)=>{" +
                "return record.p_mfgr.equals(\"MFGR#1\") || record.p_mfgr.equals(\"MFGR#2\");" +
                "}"));

        scanPart.addChild(selectionPart);
        Projection partProjecton = new Projection(new FieldReference("p_partkey", Int_32.class));
        selectionPart.addChild(partProjecton);


        Scan scanCustomer = new Scan("table.sbb_customer");

        Selection selectionCustomer = new Selection(
                new Predicate.Equal<>(
                        new FieldReference<>("c_region", Text.class),
                        new FieldConstant<>(new StringText("AMERICA", 12))
                )
        );
        scanCustomer.addChild(selectionCustomer);
        Projection customerProjecton = new Projection(new FieldReference("c_custkey", Int_32.class), new FieldReference("c_nation", Text.class, 15));
        selectionCustomer.addChild(customerProjecton);


        Scan scanSupplier = new Scan("table.sbb_supplier");

        Selection selectionSupplier = new Selection(
                new Predicate.Equal<>(
                        new FieldReference<>("s_region", Text.class),
                        new FieldConstant<>(new StringText("AMERICA", 12))
                )
        );
        scanSupplier.addChild(selectionSupplier);
        Projection projectSupplier = new Projection(new FieldReference("s_suppkey", Int_32.class));
        selectionSupplier.addChild(projectSupplier);


        Scan scan4 = new Scan("table.sbb_lineorder");

        Join join1 = new Join.EqualJoin<>(378, new FieldReference<>("s_suppkey", Int_32.class), new FieldReference<>("lo_suppkey", Int_32.class));
        projectSupplier.addChild(join1);
        scan4.addChild(join1);

        Join join2 = new Join.EqualJoin<>(5992, new FieldReference<>("c_custkey", Int_32.class), new FieldReference<>("lo_custkey", Int_32.class));
        customerProjecton.addChild(join2);
        join1.addChild(join2);

        Join join3 = new Join.EqualJoin<>(80045, new FieldReference<>("p_partkey", Int_32.class), new FieldReference<>("lo_partkey", Int_32.class));
        partProjecton.addChild(join3);
        join2.addChild(join3);

        Join join4 = new Join.EqualJoin<>(2556, new FieldReference<>("d_datekey", Int_32.class), new FieldReference<>("lo_orderdate", Int_32.class));
        projectDate.addChild(join4);
        join3.addChild(join4);


        GroupBy groupBy = new GroupBy(new KeyGroup(
                new FieldReference("d_year", Int_32.class),
                new FieldReference("c_nation", Text.class, 15)),
                new Aggregation.Sum(new FieldReference<>("lo_revenue", Numeric.class, 2)));
        join4.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        join4.addChild(sink);
        return sink;
    }

    public static LogicalOperator javaQuery() {
        Scan scanDate = new Scan("table.sbb_date");

        Projection projectDate = new Projection(new FieldReference("d_year", Int_32.class),
                new FieldReference("d_datekey", Int_32.class));
        scanDate.addChild(projectDate);

        Scan scanPart = new Scan("table.sbb_part");
        UDFOperator selectionPart = new JavaDynamicUDFOperator(javafilterTypedUDF);
        scanPart.addChild(selectionPart);
        Projection partProjecton = new Projection(new FieldReference("p_partkey", Int_32.class));
        selectionPart.addChild(partProjecton);


        Scan scanCustomer = new Scan("table.sbb_customer");

        Selection selectionCustomer = new Selection(
                new Predicate.Equal<>(
                        new FieldReference<>("c_region", Text.class),
                        new FieldConstant<>(new StringText("AMERICA", 12))
                )
        );
        scanCustomer.addChild(selectionCustomer);
        Projection customerProjecton = new Projection(new FieldReference("c_custkey", Int_32.class), new FieldReference("c_nation", Text.class, 15));
        selectionCustomer.addChild(customerProjecton);


        Scan scanSupplier = new Scan("table.sbb_supplier");

        Selection selectionSupplier = new Selection(
                new Predicate.Equal<>(
                        new FieldReference<>("s_region", Text.class),
                        new FieldConstant<>(new StringText("AMERICA", 12))
                )
        );
        scanSupplier.addChild(selectionSupplier);
        Projection projectSupplier = new Projection(new FieldReference("s_suppkey", Int_32.class));
        selectionSupplier.addChild(projectSupplier);


        Scan scan4 = new Scan("table.sbb_lineorder");

        Join join1 = new Join.EqualJoin<>(378, new FieldReference<>("s_suppkey", Int_32.class), new FieldReference<>("lo_suppkey", Int_32.class));
        projectSupplier.addChild(join1);
        scan4.addChild(join1);

        Join join2 = new Join.EqualJoin<>(5992, new FieldReference<>("c_custkey", Int_32.class), new FieldReference<>("lo_custkey", Int_32.class));
        customerProjecton.addChild(join2);
        join1.addChild(join2);

        Join join3 = new Join.EqualJoin<>(80045, new FieldReference<>("p_partkey", Int_32.class), new FieldReference<>("lo_partkey", Int_32.class));
        partProjecton.addChild(join3);
        join2.addChild(join3);

        Join join4 = new Join.EqualJoin<>(2556, new FieldReference<>("d_datekey", Int_32.class), new FieldReference<>("lo_orderdate", Int_32.class));
        projectDate.addChild(join4);
        join3.addChild(join4);


        GroupBy groupBy = new GroupBy(new KeyGroup(
                new FieldReference("d_year", Int_32.class),
                new FieldReference("c_nation", Text.class, 15)),
                new Aggregation.Sum(new FieldReference<>("lo_revenue", Numeric.class, 2)));
        join4.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        join4.addChild(sink);
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
