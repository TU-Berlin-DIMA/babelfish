package de.tub.dima.babelfish.ir.pqp.nodes.records;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.ir.pqp.nodes.BFBaseNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.typesytem.BFType;

public class WriteLuthFieldNode extends BFBaseNode {

    protected final FrameSlot inputObjectSlot;
    protected final FrameSlot valueObjectSlot;

    private final String fieldName;

    @CompilerDirectives.CompilationFinal
    private int fieldIndex = -1;

    public WriteLuthFieldNode(String fieldName, FrameSlot inputObjectSlot, FrameSlot valueObjectSlot) {
        this.fieldName = fieldName;
        this.inputObjectSlot = inputObjectSlot;
        this.valueObjectSlot = valueObjectSlot;
    }

    public Object execute(VirtualFrame frame) {
        try {
            BFRecord record = (BFRecord) frame.getObject(inputObjectSlot);
            BFType value = (BFType) frame.getObject(valueObjectSlot);
            if (CompilerDirectives.inInterpreter() && fieldIndex == -1) {
                RecordSchema schema = record.getObjectSchema();
                if (!schema.containsField(fieldName)) {
                    schema.addField(fieldName);
                }
                fieldIndex = schema.getFieldIndex(fieldName);
            }
            record.setValue(fieldIndex, value);
        } catch (FrameSlotTypeException e) {
            e.printStackTrace();
        }
        return null;
    }
}
