package de.tub.dima.babelfish.ir.pqp.nodes.state.map.hash;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.RootNode;
import de.tub.dima.babelfish.ir.pqp.nodes.records.ReadLuthFieldNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.TypedCallNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;

/**
 * Primitive IR node to calculate the hash of a intermediate {@link BFRecord}.
 */
@NodeChild(value = "value", type = ArgumentReadNode.class)
public abstract class HashBFRecordNode extends RootNode {

    private final FrameSlot inputSlot;

    public HashBFRecordNode() {
        super(null, new FrameDescriptor());
        inputSlot = getFrameDescriptor().findOrAddFrameSlot("inputObject");
    }

    public static TypedCallNode<Long> create() {
        return new TypedCallNode<>(HashBFRecordNodeGen.create(new ArgumentReadNode(0)));
    }

    /**
     * Calculate the hash-code for the input {@link BFRecord}.
     * The hash is derived by the combination of the hashes of all fields.
     *
     * @param value
     * @param hashValueNodes
     * @return
     */
    @Specialization
    @ExplodeLoop
    public long getHash(BFRecord value, @Cached(value = "getSize(value)", allowUncached = false) HashBFValueNode[] hashValueNodes) {
        VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(null, getFrameDescriptor());
        frame.setObject(inputSlot, value);
        long hash = 902850234L;
        for (int i = 0; i < hashValueNodes.length; i++) {
            hash ^= (long) hashValueNodes[i].execute(frame);
        }
        return hash;
    }

    HashBFValueNode[] getSize(BFRecord record) {
        String[] fieldNames = record.getObjectSchema().fieldNames;
        int size = record.getObjectSchema().getSize();
        HashBFValueNode[] hashValueNodes = new HashBFValueNode[size];
        for (int i = 0; i < size; i++) {
            hashValueNodes[i] = HashBFValueNode.create(new ReadLuthFieldNode(fieldNames[i], inputSlot));
        }
        return hashValueNodes;
    }


}
