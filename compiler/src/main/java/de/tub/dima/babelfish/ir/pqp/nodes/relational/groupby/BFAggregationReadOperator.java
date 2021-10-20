package de.tub.dima.babelfish.ir.pqp.nodes.relational.groupby;


import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.memory.BFLoadRecordFromAddressNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFPointerStateVariable;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;

/**
 * BF Operator, which reads one state variable from the state manager and pushes the value to the next operator.
 * Used in combination with a {@link BFGroupByOperator} for global aggregations.
 */
@NodeInfo(shortName = "BFAggregationReadOperator")
public class BFAggregationReadOperator extends BFOperator {

    private final AggregationContext context;
    @Child
    private BFLoadRecordFromAddressNode loadRecordNode;

    public BFAggregationReadOperator(TruffleLanguage<?> language,
                                     FrameDescriptor frameDescriptor,
                                     BFOperator next,
                                     StateDescriptor stateDescriptor, AggregationContext context) {
        super(language, frameDescriptor, next);
        this.loadRecordNode = new BFLoadRecordFromAddressNode(stateDescriptor);
        this.context = context;
    }


    @Override
    public void execute(VirtualFrame frame) {
        BFStateManager stateManager = (BFStateManager) frame.getValue(stateManagerSlot);
        BFPointerStateVariable pointerStateVariable = (BFPointerStateVariable) stateManager.getStateVariable(context.getIndex());
        BFRecord object = loadRecordNode.readRecord(pointerStateVariable.getAddress());
        callNextExecute(object, frame.getValue(stateManagerSlot));
    }
}
