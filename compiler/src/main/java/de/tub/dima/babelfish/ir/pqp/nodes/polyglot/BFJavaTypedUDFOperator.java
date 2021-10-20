package de.tub.dima.babelfish.ir.pqp.nodes.polyglot;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.Collector;
import de.tub.dima.babelfish.ir.lqp.udf.java.JavaTypedUDFOperator;
import de.tub.dima.babelfish.ir.lqp.udf.java.RecordTranslator;
import de.tub.dima.babelfish.ir.lqp.udf.java.typed.TypedUDF;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.records.BFGetFieldIndexNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.schema.field.SchemaField;

/**
 * BF currently supports Java UDFs as custome Operator nodes.
 * In the future we will migrate to Espresso.
 */
@NodeInfo(shortName = "BFJavaTypedUDFOperator")
public class BFJavaTypedUDFOperator extends BFOperator {

    private final JavaTypedUDFOperator udfOperator;
    //private final SerializeCollector callback;
    private final TypedUDF udf;

    @Children
    private BFGetFieldIndexNode[] getFieldIndexNodes;


    @CompilerDirectives.CompilationFinal
    private RecordSchema schema;


    public BFJavaTypedUDFOperator(TruffleLanguage<?> language,
                                  FrameDescriptor frameDescriptor,
                                  JavaTypedUDFOperator udfOperator,
                                  BFOperator nextOperator) {
        super(language, frameDescriptor);
        this.udfOperator = udfOperator;


        //this.outputCollector = new PhysicalUDFCollector(nextOperator);
        //callback = new SerializeCollector(outputCollector);
        udf = (TypedUDF) udfOperator.getUdf();
        udfOperator.init(null);
        getFieldIndexNodes = new BFGetFieldIndexNode[udfOperator.inputSchemaFields.length];
        for (int i = 0; i < udfOperator.inputSchemaFields.length; i++) {
            getFieldIndexNodes[i] = new BFGetFieldIndexNode(udfOperator.inputSchemaFields[i].getName());
        }
    }

    @Override
    public void execute(VirtualFrame frame) {

        PhysicalUDFCollector outputCollector = new PhysicalUDFCollector();
        SerializeCollector callback = new SerializeCollector(outputCollector, udfOperator.outputSchemaFields);

        BFRecord dynamicInputObject = (BFRecord) frame.getValue(inputObjectSlot);
        Record typedRecordObject = udfOperator.createTypedRecord();
        copyLuthFieldsToTypedRecord(dynamicInputObject, typedRecordObject);
        udf.call(frame, typedRecordObject, callback);

    }

    @ExplodeLoop
    public void copyLuthFieldsToTypedRecord(BFRecord input, Record record) {

        for (int i = 0; i < udfOperator.inputSchemaFields.length; i++) {
            int index = getFieldIndexNodes[i].getFieldIndex(input.getObjectSchema());
            BFType value = input.getValue(index);
            SchemaField schemaField = udfOperator.inputSchemaFields[i];
            RecordTranslator.setLuthFieldToTypedRecord(record, schemaField, udfOperator.typedInputRecordFields[schemaField.getIndex()], value);
        }
    }


    private class SerializeCollector implements Collector {

        private final UDFOperator.OutputCollector collector;
        @CompilerDirectives.CompilationFinal(dimensions = 1)
        private final SchemaField[] schema;

        public SerializeCollector(UDFOperator.OutputCollector outputCollector, SchemaField[] schema) {
            this.collector = outputCollector;
            this.schema = schema;
        }

        @Override
        public void emit(VirtualFrame frame, Record object) {
            DynamicRecord resultRecord = collector.createOutputRecord();
            copyTypedRecordFieldsToLuthRecord(object, resultRecord);
            collector.emitRecord(frame, resultRecord);
        }

        @ExplodeLoop
        public void copyTypedRecordFieldsToLuthRecord(Record record, DynamicRecord resultRecord) {
            for (SchemaField schemaField : schema) {
                String name = schemaField.getName();
                BFType value = RecordTranslator.getLuthTypedValueFromField(record, schemaField, udfOperator.typedOutputRecordFields[schemaField.getIndex()]);
                resultRecord.setValue(name, value);
            }
        }
    }


    private class PhysicalUDFCollector implements UDFOperator.OutputCollector {

        private final RecordSchema resultSchema;

        PhysicalUDFCollector() {
            this.resultSchema = new RecordSchema();
        }

        @Override
        public BFRecord createOutputRecord() {
            return BFRecord.createObject(resultSchema);
        }

        @Override
        public void emitRecord(VirtualFrame frame, DynamicRecord dynamicRecord) {
            callNextExecute(dynamicRecord, frame);
            //next.call(dynamicRecord, frame.getArguments()[1]);
        }

    }

}
