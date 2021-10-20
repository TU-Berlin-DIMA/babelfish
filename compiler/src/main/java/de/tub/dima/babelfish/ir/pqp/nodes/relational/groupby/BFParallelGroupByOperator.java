package de.tub.dima.babelfish.ir.pqp.nodes.relational.groupby;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.memory.BFLoadRecordFromAddressNode;
import de.tub.dima.babelfish.ir.pqp.nodes.memory.BFWriteRecordFromAddressNode;
import de.tub.dima.babelfish.ir.pqp.nodes.state.BFCreateStateVariableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.state.BFTransactionNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFPointerStateVariable;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.typesytem.BFType;

/**
 * BF Operator for global aggregations with support for multithreaded state updates.
 * Performs a aggregation across all input records and stores the value in a state variable.
 */
@NodeInfo(shortName = "BFParallelGroupBy")
public class BFParallelGroupByOperator extends BFOperator {

    private final FrameSlot stateVarFrameSlot;
    private final StateDescriptor stateDescriptor;
    @Child
    private BFTransactionNode.StartTransactionNode startTransactionNode;
    @Child
    private BFTransactionNode.EndTransactionNode endTransactionNode;
    @Child
    private BFLoadRecordFromAddressNode loadRecordNode;
    @Child
    private BFWriteRecordFromAddressNode writeRecordNode;
    @Child
    private BFCreateStateVariableNode createStateVariableNode;
    @Children
    private AggregationNode[] aggNode;

    public BFParallelGroupByOperator(TruffleLanguage<?> language, FrameDescriptor frameDescriptor, AggregationNode[] aggNode, StateDescriptor stateDescriptor) {
        super(language, frameDescriptor);
        this.aggNode = aggNode;
        this.stateVarFrameSlot = frameDescriptor.findOrAddFrameSlot("statevar");
        this.stateDescriptor = stateDescriptor;
        this.loadRecordNode = new BFLoadRecordFromAddressNode(stateDescriptor);
        this.createStateVariableNode = new BFCreateStateVariableNode(stateDescriptor);
        this.writeRecordNode = new BFWriteRecordFromAddressNode(stateDescriptor);
        this.startTransactionNode = new BFTransactionNode.StartTransactionNode();
        this.endTransactionNode = new BFTransactionNode.EndTransactionNode();
    }

    @Override
    public void open(VirtualFrame frame) {
        super.open(frame);
        BFStateManager stateManager = (BFStateManager) frame.getValue(stateManagerSlot);
        long stateVariableAddress = UnsafeUtils.UNSAFE.allocateMemory(stateDescriptor.getPhysicalSize());
        BFPointerStateVariable pointerStateVariable = new BFPointerStateVariable(stateVariableAddress);
        stateManager.setStateVariable(0, pointerStateVariable);

        BFRecord stateVariable = createStateVariableNode.initStateVariable();
        writeRecordNode.writeRecord(pointerStateVariable.getAddress(), stateVariable);
    }

    @Override
    public void execute(VirtualFrame frame) {
        BFStateManager stateManager = (BFStateManager) frame.getValue(stateManagerSlot);
        BFPointerStateVariable pointerStateVariable = (BFPointerStateVariable) stateManager.getStateVariable(0);
        // In the parallel case we use the first 8 byte of the state value to store a lock.
        startTransactionNode.start(pointerStateVariable.getAddress());
        BFRecord stateBFRecord = loadRecordNode.readRecord(pointerStateVariable.getAddress());
        frame.setObject(stateVarFrameSlot, stateBFRecord);
        aggregateAggregates(frame);
        writeRecordNode.writeRecord(pointerStateVariable.getAddress(), stateBFRecord);
        endTransactionNode.end(pointerStateVariable.getAddress());
    }

    @Override
    public void close(VirtualFrame frame) {
        super.close(frame);
    }


    @ExplodeLoop
    public void aggregateAggregates(VirtualFrame frame) {
        BFRecord state = (BFRecord) frame.getValue(stateVarFrameSlot);
        for (int i = 0; i < aggNode.length; i++) {
            AggregationNode agg = aggNode[i];
            BFType value = agg.execute(frame);
            state.setValue(i, value);
        }
    }
}
