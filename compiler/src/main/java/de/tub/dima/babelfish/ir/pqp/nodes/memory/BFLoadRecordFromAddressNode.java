package de.tub.dima.babelfish.ir.pqp.nodes.memory;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.pqp.nodes.BFBaseNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;
import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.typesytem.BFType;

/**
 * The BFLoadRecordFromAddressNode loads a Luth object from memory according to a physical schema.
 */
@NodeInfo(shortName = "BFLoadRecordFromAddressNode")
public class BFLoadRecordFromAddressNode extends BFBaseNode {

    @CompilerDirectives.CompilationFinal
    private final StateDescriptor stateDescriptor;

    public BFLoadRecordFromAddressNode(StateDescriptor stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    @ExplodeLoop
    public BFRecord readRecord(long address) {
        BFRecord object = BFRecord.createObject(stateDescriptor.getSchema());
        for (int i = 0; i < stateDescriptor.getPhysicalSchema().getSize(); i++) {
            PhysicalField field = stateDescriptor.getPhysicalSchema().getField(i);
            long inBufferOffset = address + stateDescriptor.getPhysicalSchema().getRecordOffset(i);
            BFType value = field.readValue(new AddressPointer(inBufferOffset));
            object.setValue(i, value);
        }
        return object;
    }

}
