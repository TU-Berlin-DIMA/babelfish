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

public class SSBQuery21 {

    static DynamicFilterFunction javafilterTypedUDF = new DynamicFilterFunction() {
        @Override
        public boolean filter(DynamicRecord record) {
            AbstractRope s_region = record.getValue("s_region");
            return s_region.equals(new StringText("AMERICA", 10));
        }
    };

    public static LogicalOperator relationalQuery() {
        Scan scanDate = new Scan("table.sbb_date");
        Projection projectDate = new Projection(new FieldReference("d_year", Int_32.class), new FieldReference("d_datekey", Int_32.class));
        scanDate.addChild(projectDate);

        Scan scanSupplier = new Scan("table.sbb_supplier");

        Selection selectionSupplier = new Selection(
                new Predicate.Equal(
                        new FieldReference<>("s_region", Text.class),
                        new FieldConstant<>(new StringText("AMERICA", 12))
                )
        );

        scanSupplier.addChild(selectionSupplier);
        Projection projectSupplier = new Projection(new FieldReference("s_suppkey", Int_32.class));
        selectionSupplier.addChild(projectSupplier);


        Scan scanPart = new Scan("table.sbb_part");
        Selection selectionPart = new Selection(
                new Predicate.Equal(
                        new FieldReference<>("p_category", Text.class),
                        new FieldConstant<>(new StringText("MFGR#12", 7))
                )
        );
        scanPart.addChild(selectionPart);
        Projection partProjecton = new Projection(new FieldReference("p_partkey", Int_32.class), new FieldReference("p_brand1", Text.class, 7));
        selectionPart.addChild(partProjecton);


        Scan scan4 = new Scan("table.sbb_lineorder");

        Join join3 = new Join.EqualJoin<>(new FieldReference<>("p_partkey", Int_32.class), new FieldReference<>("lo_partkey", Int_32.class));
        partProjecton.addChild(join3);
        scan4.addChild(join3);

        Join join4 = new Join.EqualJoin<>(new FieldReference<>("s_suppkey", Int_32.class), new FieldReference<>("lo_suppkey", Int_32.class));
        projectSupplier.addChild(join4);
        join3.addChild(join4);

        Join join5 = new Join.EqualJoin<>(new FieldReference<>("d_datekey", Int_32.class), new FieldReference<>("lo_orderdate", Int_32.class));
        projectDate.addChild(join5);
        join4.addChild(join5);


        GroupBy groupBy = new GroupBy(new KeyGroup(
                new FieldReference("d_year", Int_32.class),
                new FieldReference("p_brand1", Text.class, 9)),
                new Aggregation.Sum(new FieldReference<>("lo_revenue", Numeric.class, 2)));
        join5.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator pythonQuery() {
        Scan scanDate = new Scan("table.sbb_date");
        Projection projectDate = new Projection(new FieldReference("d_year", Int_32.class), new FieldReference("d_datekey", Int_32.class));
        scanDate.addChild(projectDate);

        Scan scanSupplier = new Scan("table.sbb_supplier");

        PythonOperator selectionSupplier = new PythonOperator(new PythonUDF(
                "lambda rec,ctx: (rec.s_region.equals(\"AMERICA\"))"));
        scanSupplier.addChild(selectionSupplier);

        Projection projectSupplier = new Projection(new FieldReference("s_suppkey", Int_32.class));
        selectionSupplier.addChild(projectSupplier);


        Scan scanPart = new Scan("table.sbb_part");
        Selection selectionPart = new Selection(
                new Predicate.Equal(
                        new FieldReference<>("p_category", Text.class),
                        new FieldConstant<>(new StringText("MFGR#12", 7))
                )
        );
        scanPart.addChild(selectionPart);
        Projection partProjecton = new Projection(new FieldReference("p_partkey", Int_32.class), new FieldReference("p_brand1", Text.class, 7));
        selectionPart.addChild(partProjecton);


        Scan scan4 = new Scan("table.sbb_lineorder");

        Join join3 = new Join.EqualJoin<>(new FieldReference<>("p_partkey", Int_32.class), new FieldReference<>("lo_partkey", Int_32.class));
        partProjecton.addChild(join3);
        scan4.addChild(join3);

        Join join4 = new Join.EqualJoin<>(new FieldReference<>("s_suppkey", Int_32.class), new FieldReference<>("lo_suppkey", Int_32.class));
        projectSupplier.addChild(join4);
        join3.addChild(join4);

        Join join5 = new Join.EqualJoin<>(new FieldReference<>("d_datekey", Int_32.class), new FieldReference<>("lo_orderdate", Int_32.class));
        projectDate.addChild(join5);
        join4.addChild(join5);


        GroupBy groupBy = new GroupBy(new KeyGroup(
                new FieldReference("d_year", Int_32.class),
                new FieldReference("p_brand1", Text.class, 9)),
                new Aggregation.Sum(new FieldReference<>("lo_revenue", Numeric.class, 2)));
        join5.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator javaScriptQuery() {
        Scan scanDate = new Scan("table.sbb_date");
        Projection projectDate = new Projection(new FieldReference("d_year", Int_32.class), new FieldReference("d_datekey", Int_32.class));
        scanDate.addChild(projectDate);

        Scan scanSupplier = new Scan("table.sbb_supplier");

        JavaScriptOperator selectionSupplier = new JavaScriptOperator(new JavaScriptUDF("(record,ctx)=>{" +
                "return record.s_region.equals(\"AMERICA\");" +
                "}"));

        scanSupplier.addChild(selectionSupplier);
        Projection projectSupplier = new Projection(new FieldReference("s_suppkey", Int_32.class));
        selectionSupplier.addChild(projectSupplier);


        Scan scanPart = new Scan("table.sbb_part");
        Selection selectionPart = new Selection(
                new Predicate.Equal(
                        new FieldReference<>("p_category", Text.class),
                        new FieldConstant<>(new StringText("MFGR#12", 7))
                )
        );
        scanPart.addChild(selectionPart);
        Projection partProjecton = new Projection(new FieldReference("p_partkey", Int_32.class), new FieldReference("p_brand1", Text.class, 7));
        selectionPart.addChild(partProjecton);


        Scan scan4 = new Scan("table.sbb_lineorder");

        Join join3 = new Join.EqualJoin<>(new FieldReference<>("p_partkey", Int_32.class), new FieldReference<>("lo_partkey", Int_32.class));
        partProjecton.addChild(join3);
        scan4.addChild(join3);

        Join join4 = new Join.EqualJoin<>(new FieldReference<>("s_suppkey", Int_32.class), new FieldReference<>("lo_suppkey", Int_32.class));
        projectSupplier.addChild(join4);
        join3.addChild(join4);

        Join join5 = new Join.EqualJoin<>(new FieldReference<>("d_datekey", Int_32.class), new FieldReference<>("lo_orderdate", Int_32.class));
        projectDate.addChild(join5);
        join4.addChild(join5);


        GroupBy groupBy = new GroupBy(new KeyGroup(
                new FieldReference("d_year", Int_32.class),
                new FieldReference("p_brand1", Text.class, 9)),
                new Aggregation.Sum(new FieldReference<>("lo_revenue", Numeric.class, 2)));
        join5.addChild(groupBy);


        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator javaQuery() {
        Scan scanDate = new Scan("table.sbb_date");
        Projection projectDate = new Projection(new FieldReference("d_year", Int_32.class), new FieldReference("d_datekey", Int_32.class));
        scanDate.addChild(projectDate);

        Scan scanSupplier = new Scan("table.sbb_supplier");

        UDFOperator selectionSupplier = new JavaDynamicUDFOperator(javafilterTypedUDF);


        scanSupplier.addChild(selectionSupplier);
        Projection projectSupplier = new Projection(new FieldReference("s_suppkey", Int_32.class));
        selectionSupplier.addChild(projectSupplier);


        Scan scanPart = new Scan("table.sbb_part");
        Selection selectionPart = new Selection(
                new Predicate.Equal(
                        new FieldReference<>("p_category", Text.class),
                        new FieldConstant<>(new StringText("MFGR#12", 7))
                )
        );
        scanPart.addChild(selectionPart);
        Projection partProjecton = new Projection(new FieldReference("p_partkey", Int_32.class), new FieldReference("p_brand1", Text.class, 7));
        selectionPart.addChild(partProjecton);


        Scan scan4 = new Scan("table.sbb_lineorder");

        Join join3 = new Join.EqualJoin<>(new FieldReference<>("p_partkey", Int_32.class), new FieldReference<>("lo_partkey", Int_32.class));
        partProjecton.addChild(join3);
        scan4.addChild(join3);

        Join join4 = new Join.EqualJoin<>(new FieldReference<>("s_suppkey", Int_32.class), new FieldReference<>("lo_suppkey", Int_32.class));
        projectSupplier.addChild(join4);
        join3.addChild(join4);

        Join join5 = new Join.EqualJoin<>(new FieldReference<>("d_datekey", Int_32.class), new FieldReference<>("lo_orderdate", Int_32.class));
        projectDate.addChild(join5);
        join4.addChild(join5);


        GroupBy groupBy = new GroupBy(new KeyGroup(
                new FieldReference("d_year", Int_32.class),
                new FieldReference("p_brand1", Text.class, 9)),
                new Aggregation.Sum(new FieldReference<>("lo_revenue", Numeric.class, 2)));
        join5.addChild(groupBy);


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
