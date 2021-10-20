package de.tub.dima.babelfish.ir.pqp.nodes.records;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_32;

public class ReadLuthFieldNode extends BFExecutableNode {

    protected final FrameSlot inputObjectSlot;

    private final String fieldName;

    @CompilerDirectives.CompilationFinal
    private int fieldIndex = -1;


    public ReadLuthFieldNode(String fieldName, FrameSlot inputObjectSlot) {
        this.fieldName = fieldName;
        this.inputObjectSlot = inputObjectSlot;
    }

    public Object execute(VirtualFrame frame) {
        try {
            BFRecord value = (BFRecord) frame.getObject(inputObjectSlot);
            if (CompilerDirectives.inInterpreter() && fieldIndex == -1) {
                RecordSchema schema = value.getObjectSchema();
                fieldIndex = schema.getFieldIndex(fieldName);
                if (fieldIndex == -1)
                    throw new RuntimeException("Field: " + fieldName + " no found.");
            }
            return value.getValue(fieldIndex, Eager_Int_32.class);
        } catch (FrameSlotTypeException e) {
            e.printStackTrace();
        }
        return null;
    }
}
