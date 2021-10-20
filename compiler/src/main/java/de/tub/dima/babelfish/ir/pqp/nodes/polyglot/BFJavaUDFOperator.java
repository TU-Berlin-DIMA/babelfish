package de.tub.dima.babelfish.ir.pqp.nodes.polyglot;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.MaterializedFrame;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.lqp.udf.UDFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;

/**
 * BF currently supports Java UDFs as custome Operator nodes.
 * In the future we will migrate to Espresso.
 */
@NodeInfo(shortName = "BFJavaTypedUDFOperator")
public class BFJavaUDFOperator extends BFOperator {

    private final UDFOperator udfOperator;

    @Child
    private PhysicalUDFCollector outputCollector;


    @CompilerDirectives.CompilationFinal
    private RecordSchema schema;


    public BFJavaUDFOperator(TruffleLanguage<?> language,
                             FrameDescriptor frameDescriptor,
                             UDFOperator udfOperator,
                             BFOperator nextOperator) {
        super(language, frameDescriptor, nextOperator);
        this.udfOperator = udfOperator;
        this.outputCollector = new PhysicalUDFCollector();
        udfOperator.init(outputCollector);
    }

    @Override
    public void execute(VirtualFrame frame) {
        BFRecord object = (BFRecord) frame.getValue(inputObjectSlot);
        if (CompilerDirectives.inInterpreter())
            schema = object.getObjectSchema();
        Wrapper wrapper = new Wrapper(object, schema);
        udfOperator.execute(frame, wrapper, outputCollector);
    }


    private class Wrapper extends DynamicRecord {
        private final BFRecord object;
        private final RecordSchema localSchema;

        private Wrapper(BFRecord object, RecordSchema schema) {
            this.object = object;
            localSchema = schema;
        }

        public void setValue(String string, BFType value) {
            int index = localSchema.getFieldIndexFromConstant(string);
            object.setValue(index, value);
        }

        public <T extends BFType> T getValue(String string) {
            int index = localSchema.getFieldIndexFromConstant(string);
            return object.getValue(index);
        }

    }

    private class PhysicalUDFCollector extends Node implements UDFOperator.OutputCollector {

        private final RecordSchema resultSchema;
        public MaterializedFrame globalFrame;


        PhysicalUDFCollector() {
            this.resultSchema = new RecordSchema();
        }

        @Override
        public BFRecord createOutputRecord() {
            return BFRecord.createObject(resultSchema);
        }

        @Override
        public void emitRecord(VirtualFrame frame, DynamicRecord dynamicRecord) {
            if (dynamicRecord instanceof Wrapper) {
                callNextExecute(((Wrapper) dynamicRecord).object, frame.getArguments()[1]);
            } else {
                callNextExecute(dynamicRecord, frame.getArguments()[1]);
            }

        }

    }


}
