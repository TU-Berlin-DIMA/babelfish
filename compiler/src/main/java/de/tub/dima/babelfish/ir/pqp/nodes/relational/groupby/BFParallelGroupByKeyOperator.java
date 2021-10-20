package de.tub.dima.babelfish.ir.pqp.nodes.relational.groupby;

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
import de.tub.dima.babelfish.ir.pqp.nodes.state.BFTransactionNode;
import de.tub.dima.babelfish.ir.pqp.nodes.state.map.HashMapConcurrentFindEntryNode;
import de.tub.dima.babelfish.ir.pqp.nodes.state.map.HashMapReadValue;
import de.tub.dima.babelfish.ir.pqp.nodes.state.map.HashMapWriteValue;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.TypedCallNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;
import de.tub.dima.babelfish.ir.pqp.objects.state.map.DynHashMap;
import de.tub.dima.babelfish.typesytem.BFType;

/**
 * BF Operator for global aggregations with support for multithreaded state updates.
 * Performs a aggregation across all input records and stores the value in a state variable.
 */
@NodeInfo(shortName = "BFParallelGroupByKey")
public class BFParallelGroupByKeyOperator extends BFOperator {

    private final AggregationContext context;
    private final FrameSlot stateVarFrameSlot;
    private final FrameSlot valueSlot;
    private final StateDescriptor stateDescriptor;
    @Child
    TypedCallNode<BFRecord> readHashMapValueNode;
    @Child
    TypedCallNode<Long> findEntryNode;
    @Child
    private BFTransactionNode.StartTransactionNode startTransactionNode;
    @Child
    private BFTransactionNode.EndTransactionNode endTransactionNode;
    @Children
    private WriteLuthFieldNode[] aggFieldWriteNodes;
    @Child
    private TypedCallNode<Void> writeHashMapValueNode;
    @Children
    private AggregationNode[] aggNode;


    public BFParallelGroupByKeyOperator(TruffleLanguage<?> language,
                                        FrameDescriptor frameDescriptor,
                                        AggregationNode[] aggNode,
                                        KeyGroup key,
                                        StateDescriptor stateDescriptor,
                                        AggregationContext context) {
        super(language, frameDescriptor);
        this.aggNode = aggNode;
        this.stateVarFrameSlot = frameDescriptor.findOrAddFrameSlot("statevar");
        this.stateDescriptor = stateDescriptor;
        this.context = context;
        this.readHashMapValueNode = HashMapReadValue.create(stateDescriptor.getPhysicalSchema());
        this.writeHashMapValueNode = HashMapWriteValue.create(stateDescriptor.getPhysicalSchema());
        this.aggFieldWriteNodes = new WriteLuthFieldNode[aggNode.length];
        this.valueSlot = frameDescriptor.findOrAddFrameSlot("fieldValueSlot");
        this.startTransactionNode = new BFTransactionNode.StartTransactionNode();
        this.endTransactionNode = new BFTransactionNode.EndTransactionNode();
        for (int i = 0; i < aggNode.length; i++) {
            AggregationNode agg = aggNode[i];
            aggFieldWriteNodes[i] = new WriteLuthFieldNode(agg.getPhysicalField().getName(), stateVarFrameSlot, valueSlot);
        }
        findEntryNode = HashMapConcurrentFindEntryNode.create(stateDescriptor.getPhysicalSchema(), key.getKeys().toArray(new FieldReference[0]));
    }

    @Override
    public void open(VirtualFrame frame) {
        super.open(frame);
        BFStateManager stateManager = (BFStateManager) frame.getValue(stateManagerSlot);
        // allocate intermediate hash table
        DynHashMap hashTable = new DynHashMap(context.getCardinality(), (int) stateDescriptor.getPhysicalSize());
        // set hash table to state manager
        stateManager.setStateVariable(context.getIndex(), hashTable);
    }

    @Override
    public void execute(VirtualFrame frame) {
        BFStateManager stateManager = (BFStateManager) frame.getValue(stateManagerSlot);
        BFRecord currentInputObject = (BFRecord) frame.getValue(inputObjectSlot);
        // get hashmap from state manager
        DynHashMap hashMap = (DynHashMap) stateManager.getStateVariable(context.getIndex());
        // get entry for key in in hash map
        long entryAddress = findEntryNode.call(hashMap, currentInputObject);

        long lockAddress = DynHashMap.EntryHandler.getLockAddress(entryAddress);
        startTransactionNode.start(lockAddress);
        BFRecord stateBFRecord = readHashMapValueNode.call(entryAddress);
        frame.setObject(stateVarFrameSlot, stateBFRecord);
        aggregateAggregates(frame);
        writeHashMapValueNode.call(stateBFRecord, entryAddress);
        endTransactionNode.end(lockAddress);
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
        BFRecord state = (BFRecord) frame.getValue(stateVarFrameSlot);
        for (int i = 0; i < aggNode.length; i++) {
            AggregationNode agg = aggNode[i];
            BFType value = agg.execute(frame);
            frame.setObject(valueSlot, value);
            aggFieldWriteNodes[i].execute(frame);
        }
    }

}
