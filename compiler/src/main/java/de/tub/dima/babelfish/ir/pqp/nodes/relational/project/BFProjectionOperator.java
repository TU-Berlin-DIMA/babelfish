package de.tub.dima.babelfish.ir.pqp.nodes.relational.project;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.records.ReadLuthFieldNode;
import de.tub.dima.babelfish.ir.pqp.nodes.records.WriteLuthFieldNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;

/**
 * BF Operator for projections.
 * Creates a new BFRecord with all selected fields.
 */
@NodeInfo(shortName = "BFProjectionOperator")
public class BFProjectionOperator extends BFOperator {

    @Children
    private final ReadLuthFieldNode[] readFields;

    @Children
    private final WriteLuthFieldNode[] writeFields;

    @CompilerDirectives.CompilationFinal
    private final RecordSchema schema = new RecordSchema();
    private final FrameSlot resultSlot;
    private final FrameSlot valueSlot;

    public BFProjectionOperator(TruffleLanguage<?> language,
                                FrameDescriptor frameDescriptor,
                                FieldReference[] fieldReferences,
                                BFOperator next) {
        super(language, frameDescriptor, next);
        readFields = new ReadLuthFieldNode[fieldReferences.length];
        writeFields = new WriteLuthFieldNode[fieldReferences.length];
        resultSlot = frameDescriptor.findOrAddFrameSlot("resultSlot");
        valueSlot = frameDescriptor.findOrAddFrameSlot("valueSlot");
        for (int i = 0; i < fieldReferences.length; i++) {
            readFields[i] = new ReadLuthFieldNode(fieldReferences[i].getName(), inputObjectSlot);
            writeFields[i] = new WriteLuthFieldNode(fieldReferences[i].getName(), resultSlot, valueSlot);
        }
    }

    @Override
    @ExplodeLoop
    public void execute(VirtualFrame frame) {
        BFRecord resultObject = BFRecord.createObject(schema);
        frame.setObject(resultSlot, resultObject);
        for (int i = 0; i < readFields.length; i++) {
            Object value = readFields[i].execute(frame);
            frame.setObject(valueSlot, value);
            writeFields[i].execute(frame);
        }
        callNextExecute(resultObject, frame.getValue(stateManagerSlot));
    }
}
