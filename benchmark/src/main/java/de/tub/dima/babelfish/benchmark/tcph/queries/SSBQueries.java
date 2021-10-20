package de.tub.dima.babelfish.benchmark.tcph.queries;

import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.*;
import de.tub.dima.babelfish.ir.lqp.schema.FieldConstant;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

import java.io.Serializable;

public class SSBQueries implements Serializable {


// select sum(lo_revenue), d_year, p_brand1
// from lineorder, "date", part, supplier
// where lo_orderdate = d_datekey
// and lo_partkey = p_partkey
// and lo_suppkey = s_suppkey
// and p_category = 'MFGR#12'
// and s_region = 'AMERICA'
// group by d_year, p_brand1

    //                 sort
//
//                groupby
//
//                 join
//                 hash
//
// tablescan             join
// date                  hash
//
//          tablescan        join
//          supplier         hash
//
//                    tablescan tablescan
//                    part      lineorder
    public static LogicalOperator relationalQuery21() {
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


        GroupBy groupBy = new GroupBy(new KeyGroup(new FieldReference("d_year", Int_32.class), new FieldReference("p_brand1", Text.class, 9)), new Aggregation.Sum(new FieldReference<>("lo_revenue", Float_32.class)));
        join5.addChild(groupBy);


        Sink sink = new Sink.PrintSink();
        groupBy.addChild(sink);
        return sink;
    }

}
