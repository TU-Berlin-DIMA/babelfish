package de.tub.dima.babelfish.ir.pqp.nodes.relational.groupby;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.lqp.relational.KeyGroup;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.records.WriteLuthFieldNode;
import de.tub.dima.babelfish.ir.pqp.nodes.state.map.HashMapFindEntryNode;
import de.tub.dima.babelfish.ir.pqp.nodes.state.map.HashMapReadValue;
import de.tub.dima.babelfish.ir.pqp.nodes.state.map.HashMapWriteValue;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.TypedCallNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;
import de.tub.dima.babelfish.ir.pqp.objects.state.map.DynHashMap;
import de.tub.dima.babelfish.typesytem.BFType;

/**
 * BF operator for keyed aggregations.
 * Performs a aggregation across all input records and stores the value in a hash map.
 */
@NodeInfo(shortName = "BFGroupByKey")
public class BFGroupByKeyOperator extends BFOperator {

    private final FrameSlot stateVarFrameSlot;
    private final AggregationContext context;
    private final FrameSlot valueSlot;
    private final StateDescriptor stateDescriptor;
    @Child
    TypedCallNode<BFRecord> readHashMapValueNode;
    @Child
    TypedCallNode<Long> findEntryInHashTableNode;
    @Children
    private WriteLuthFieldNode[] aggFieldWriteNodes;
    @Child
    private TypedCallNode<Void> writeHashMapValueNode;
    @Children
    private AggregationNode[] aggNode;

    public BFGroupByKeyOperator(TruffleLanguage<?> language,
                                FrameDescriptor frameDescriptor,
                                AggregationNode[] aggNode,
                                KeyGroup key,
                                StateDescriptor stateDescriptor,
                                AggregationContext context) {
        super(language, frameDescriptor);
        this.stateVarFrameSlot = frameDescriptor.findOrAddFrameSlot("statevar");
        this.aggNode = aggNode;
        this.stateDescriptor = stateDescriptor;
        this.readHashMapValueNode = HashMapReadValue.create(stateDescriptor.getPhysicalSchema());
        this.writeHashMapValueNode = HashMapWriteValue.create(stateDescriptor.getPhysicalSchema());
        this.aggFieldWriteNodes = new WriteLuthFieldNode[aggNode.length];
        this.valueSlot = frameDescriptor.findOrAddFrameSlot("fieldValueSlot");
        for (int i = 0; i < aggNode.length; i++) {
            AggregationNode agg = aggNode[i];
            aggFieldWriteNodes[i] = new WriteLuthFieldNode(agg.getPhysicalField().getName(), stateVarFrameSlot, valueSlot);
        }
        this.context = context;
        this.findEntryInHashTableNode = HashMapFindEntryNode.create(stateDescriptor.getPhysicalSchema(), key.getKeys().toArray(new FieldReference[0]));
    }


    @Override
    public void open(VirtualFrame frame) {
        super.open(frame);
        BFStateManager stateManager = (BFStateManager) frame.getValue(stateManagerSlot);
        // allocate intermediate hash table
        if (CompilerDirectives.inInterpreter())
            System.out.println("Create DynHashMap for BFGroupByKeyOperator for index: " + context.getIndex());
        DynHashMap hashTable = new DynHashMap(context.getCardinality(), (int) stateDescriptor.getPhysicalSize());
        // set hash table to state manager
        stateManager.setStateVariable(context.getIndex(), hashTable);
    }

    @Override
    public void execute(VirtualFrame frame) {
        BFStateManager stateManager = (BFStateManager) frame.getValue(stateManagerSlot);
        BFRecord currentInputObject = (BFRecord) frame.getValue(inputObjectSlot);
        // get hashmap from state manager
        if(context.getIndex()==0)
            System.out.println("Break");
        DynHashMap hashMap = (DynHashMap) stateManager.getStateVariable(context.getIndex());
        // get entry for key in in hash map
        long entryAddress = findEntryInHashTableNode.call(hashMap, currentInputObject);
        // read value from state
        BFRecord stateObject = readHashMapValueNode.call(entryAddress);
        frame.setObject(stateVarFrameSlot, stateObject);
        // aggregate state and current input object. Both are already stored in the current frame.
        aggregateAggregates(frame);
        // write state value back to the hash table.
        writeHashMapValueNode.call(stateObject, entryAddress);
    }

    @Override
    public void close(VirtualFrame frame) {
        super.close(frame);
        BFStateManager stateManager = (BFStateManager) frame.getValue(stateManagerSlot);
        DynHashMap hashMap = (DynHashMap) stateManager.getStateVariable(context.getIndex());
        hashMap.free();
    }

    @ExplodeLoop
    public void aggregateAggregates(VirtualFrame frame) {
        for (int i = 0; i < aggNode.length; i++) {
            AggregationNode agg = aggNode[i];
            BFType value = agg.execute(frame);
            frame.setObject(valueSlot, value);
            aggFieldWriteNodes[i].execute(frame);
        }
    }
}
