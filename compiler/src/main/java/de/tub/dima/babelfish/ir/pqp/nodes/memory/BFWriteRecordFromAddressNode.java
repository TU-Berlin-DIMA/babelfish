package de.tub.dima.babelfish.ir.pqp.nodes.memory;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.pqp.nodes.BFBaseNode;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.sinks.WriteLuthTypeNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;
import de.tub.dima.babelfish.typesytem.BFType;

/**
 * The BFLoadRecordFromAddressNode loads a Luth object from memory according to a physical schema.
 */
@NodeInfo(shortName = "BFWriteRecordFromAddressNode")
public class BFWriteRecordFromAddressNode extends BFBaseNode {

    @CompilerDirectives.CompilationFinal
    private final StateDescriptor stateDescriptor;
    private final FrameDescriptor frameDescriptor;
    @Children
    private WriteLuthTypeNode[] memoryWriteNode;


    public BFWriteRecordFromAddressNode(StateDescriptor stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
        this.memoryWriteNode = new WriteLuthTypeNode[stateDescriptor.getPhysicalSchema().getSize()];
        this.frameDescriptor = new FrameDescriptor();
        for (int i = 0; i < stateDescriptor.getPhysicalSchema().getSize(); i++) {
            this.memoryWriteNode[i] = WriteLuthTypeNode.createNode(
                    new ArgumentReadNode(0),
                    new ArgumentReadNode(1));
        }
    }

    @ExplodeLoop
    public void writeRecord(long address, BFRecord record) {
        for (int i = 0; i < stateDescriptor.getPhysicalSchema().getSize(); i++) {
            long inBufferOffset = address + stateDescriptor.getPhysicalSchema().getRecordOffset(i);
            BFType value = record.getValue(i);
            VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(new Object[]{value, inBufferOffset}, frameDescriptor);
            memoryWriteNode[i].execute(frame);
        }
    }

}
