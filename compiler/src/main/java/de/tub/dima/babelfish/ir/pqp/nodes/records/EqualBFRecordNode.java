package de.tub.dima.babelfish.ir.pqp.nodes.records;

import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.RootNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.TypedCallNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;

@NodeChild(value = "left", type = ArgumentReadNode.class)
@NodeChild(value = "right", type = ArgumentReadNode.class)
public abstract class EqualBFRecordNode extends RootNode {

    protected EqualBFRecordNode() {
        super(null);
    }

    public static TypedCallNode<Boolean> create() {
        return new TypedCallNode<>(EqualBFRecordNodeGen.create(new ArgumentReadNode(0), new ArgumentReadNode(1)));
    }


    int getSize(BFRecord record) {
        return record.getObjectSchema().getSize();
    }

    TypedCallNode<Boolean>[] getEqualNodes(RecordSchema schema) {
        TypedCallNode<Boolean>[] nodes = new TypedCallNode[schema.getSize()];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = EqualLuthValueNode.create();
        }
        return nodes;
    }


    @Specialization
    @ExplodeLoop
    boolean equal(BFRecord left, BFRecord right, @Cached(value = "getSize(left)", allowUncached = true) int size,
                  @Cached(value = "getEqualNodes(left.getObjectSchema())", allowUncached = true) TypedCallNode<Boolean>[] equalNodes) {

        for (int i = 0; i < size; i++) {
            if (!equalNodes[i].call(left.getValue(i), right.getValue(i))) {
                return false;
            }
        }
        return true;
    }


}
