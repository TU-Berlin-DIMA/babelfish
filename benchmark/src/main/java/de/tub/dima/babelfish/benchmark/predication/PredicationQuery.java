package de.tub.dima.babelfish.benchmark.predication;

import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.Aggregation;
import de.tub.dima.babelfish.ir.lqp.relational.GroupBy;
import de.tub.dima.babelfish.ir.lqp.relational.Predicate;
import de.tub.dima.babelfish.ir.lqp.relational.Selection;
import de.tub.dima.babelfish.ir.lqp.schema.FieldConstant;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaDynamicUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.dynamic.DynamicFilterFunction;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptOperator;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptSelectionUDF;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonSelectionUDF;
import de.tub.dima.babelfish.storage.*;
import de.tub.dima.babelfish.storage.layout.PhysicalLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalLayoutFactory;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;
import de.tub.dima.babelfish.typesytem.record.*;
import de.tub.dima.babelfish.typesytem.schema.Schema;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Precision;

import java.util.Random;

public class PredicationQuery {

    @LuthRecord(name = "predicatedValue")
    public class IntegerValue implements Record {
        @Precision(value = 0)
        public Numeric value;
    }

    public static void createData(
            long size) throws SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();

        Schema schema = RecordUtil.createSchema(IntegerValue.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long bufferSize = ((((long) physicalSchema.getFixedRecordSize()) * size) + 1024);
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(bufferSize));
        PhysicalLayoutFactory.ColumnLayoutFactory fac = new PhysicalLayoutFactory.ColumnLayoutFactory();
        PhysicalLayout physicalVariableLengthLayout = fac.create(physicalSchema, buffer.getSize().getBytes());
        physicalVariableLengthLayout.initBuffer(buffer);
        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        Random rm = new Random(42);

        for (long i = 0; i < size; i++) {
            UnsafeUtils.putLong(buffer.getVirtualAddress().getAddress() +
                    physicalVariableLengthLayout.getFieldBufferOffset(i, 0).getAddress(), rm.nextInt(100));
            physicalVariableLengthLayout.incrementRecordNumber(buffer);
        }

        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        Catalog.getInstance().registerLayout("table.predication", physicalVariableLengthLayout);
        Catalog.getInstance().registerBuffer("table.predication", buffer);

    }

    public static LogicalOperator relSelection(int selectivity) {
        Scan scan = new Scan("table.predication");
        Selection selection = new Selection(
                new Predicate.GreaterEquals<>(
                        new FieldReference<>("value", Numeric.class, 0),
                        new FieldConstant<>(new EagerNumeric(selectivity,0))));
        scan.addChild(selection);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("value", Numeric.class, 0)));
        selection.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator javaScriptSelection(int selectivity) {
        Scan scan = new Scan("table.predication");

        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptSelectionUDF("(record,ctx)=>{" +
                "return (record.value > " + selectivity + ")" +
                "}"));
        scan.addChild(selection);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("value", Numeric.class, 0)));
        selection.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    public static LogicalOperator pythonSelection(int selectivity) {
        Scan scan = new Scan("table.predication");

        PythonOperator selection = new PythonOperator(new PythonSelectionUDF(
                "lambda rec,ctx:  rec.value>" + selectivity + ""));
        scan.addChild(selection);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("value", Numeric.class, 0)));
        selection.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

    static DynamicFilterFunction tcphfilterTypedUDF = new DynamicFilterFunction() {
        @Override
        public boolean filter(DynamicRecord record) {
            EagerNumeric value = record.getValue("value");
            return value.value > 0;
        }
    };


    public static LogicalOperator javaSelection(final int selectivity) {
        Scan scan = new Scan("table.predication");

        UDFOperator selection = new JavaDynamicUDFOperator(new DynamicFilterFunction() {
            @Override
            public boolean filter(DynamicRecord record) {
                EagerNumeric value = record.getValue("value");
                return value.value > selectivity;
            }
        });
        scan.addChild(selection);

        GroupBy groupBy = new GroupBy(new Aggregation.Sum(new FieldReference<>("value", EagerNumeric.class)));
        selection.addChild(groupBy);
        Sink sink = new Sink.MemorySink();
        groupBy.addChild(sink);
        return sink;
    }

}
