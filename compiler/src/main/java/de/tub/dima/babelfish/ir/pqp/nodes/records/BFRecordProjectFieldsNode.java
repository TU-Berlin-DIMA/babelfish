package de.tub.dima.babelfish.ir.pqp.nodes.records;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.RootNode;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;

/**
 * Primitive node to project the field of a {@link BFRecord}
 */
@NodeChild(type = ArgumentReadNode.class)
public abstract class BFRecordProjectFieldsNode extends RootNode {

    private final FrameSlot inputSlot;
    private final FrameSlot resultSlot;
    private final FrameSlot valueSlot;
    @CompilerDirectives.CompilationFinal
    private final RecordSchema schema = new RecordSchema();
    @Children
    private ReadLuthFieldNode[] readFields;
    @Children
    private WriteLuthFieldNode[] writeFields;

    protected BFRecordProjectFieldsNode(FieldReference[] fieldReferences) {
        super(null, new FrameDescriptor());
        readFields = new ReadLuthFieldNode[fieldReferences.length];
        writeFields = new WriteLuthFieldNode[fieldReferences.length];
        inputSlot = getFrameDescriptor().findOrAddFrameSlot("inputSlot");
        resultSlot = getFrameDescriptor().findOrAddFrameSlot("resultSlot");
        valueSlot = getFrameDescriptor().findOrAddFrameSlot("valueSlot");
        for (int i = 0; i < fieldReferences.length; i++) {
            readFields[i] = new ReadLuthFieldNode(fieldReferences[i].getName(), inputSlot);
            writeFields[i] = new WriteLuthFieldNode(fieldReferences[i].getName(), resultSlot, valueSlot);
            schema.addField(fieldReferences[i].getName());
        }
    }

    public static BFRecordProjectFieldsNode create(FieldReference[] references) {
        return BFRecordProjectFieldsNodeGen.create(references, new ArgumentReadNode(0));
    }

    @Specialization
    @ExplodeLoop
    public BFRecord project(BFRecord inputObject) {
        VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(null, getFrameDescriptor());
        frame.setObject(inputSlot, inputObject);
        BFRecord resultObject = BFRecord.createObject(schema);
        frame.setObject(resultSlot, resultObject);
        for (int i = 0; i < readFields.length; i++) {
            Object value = readFields[i].execute(frame);
            frame.setObject(valueSlot, value);
            writeFields[i].execute(frame);
        }
        return resultObject;
    }
}
