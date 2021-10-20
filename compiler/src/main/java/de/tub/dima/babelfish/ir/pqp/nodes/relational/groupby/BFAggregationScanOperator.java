package de.tub.dima.babelfish.ir.pqp.nodes.relational.groupby;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.TruffleRuntime;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.*;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.records.WriteLuthFieldNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;
import de.tub.dima.babelfish.ir.pqp.objects.state.map.DynHashMap;
import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.typesytem.BFType;

/**
 * Aggregation Scan operator for keyed aggregations.
 * The operator iterates over the key space an pushes each record to the next operator.
 */
@NodeInfo(shortName = "BFAggregationScanOperator")
public class BFAggregationScanOperator extends BFOperator {

    private final FrameSlot aggResultSlot;
    private final FrameSlot scanContextSlot;
    private final AggregationContext context;
    @Child
    private LoopNode scanNode;

    @Child
    private WriteLuthFieldNode field;

    public BFAggregationScanOperator(TruffleLanguage<?> language,
                                     FrameDescriptor frameDescriptor,
                                     BFOperator next,
                                     StateDescriptor stateDescriptor,
                                     AggregationContext context) {
        super(language, frameDescriptor, next);
        aggResultSlot = frameDescriptor.findOrAddFrameSlot("result");
        scanContextSlot = frameDescriptor.findOrAddFrameSlot("scanContext");
        this.context = context;
        field = new WriteLuthFieldNode("sum", inputObjectSlot, aggResultSlot);
        this.scanNode = Truffle.getRuntime().createLoopNode(new ScanRepeatingNode(frameDescriptor, next, stateDescriptor));
    }

    @Override
    public void execute(VirtualFrame frame) {
        try {
            BFStateManager stateManager = (BFStateManager) frame.getObject(stateManagerSlot);
            DynHashMap hashMap = (DynHashMap) stateManager.getStateVariable(context.getIndex());
            long max = hashMap.getMaxEntry();
            if (CompilerDirectives.inInterpreter())
                System.out.println("BFAggregationScanOperator: Group Results " + max);
            frame.setObject(scanContextSlot, new ScanContext(max));
        } catch (FrameSlotTypeException e) {
            e.printStackTrace();
        }
        scanNode.execute(frame);
    }

    public class ScanRepeatingNode extends Node implements RepeatingNode {

        private final FrameDescriptor fd;
        private final StateDescriptor stateDescriptor;
        @Child
        private DirectCallNode nextOperator;

        public ScanRepeatingNode(FrameDescriptor frameDescriptor,
                                 BFOperator next,
                                 StateDescriptor stateDescriptor) {
            TruffleRuntime runtime = Truffle.getRuntime();
            this.nextOperator = runtime.createDirectCallNode(
                    runtime.createCallTarget(
                            next.getExecuteCall()));
            fd = frameDescriptor;
            this.stateDescriptor = stateDescriptor;
        }

        @ExplodeLoop
        public BFRecord getAsLuthObject(long address) {

            BFRecord object = BFRecord.createObject(stateDescriptor.getSchema());
            for (int i = 0; i < stateDescriptor.getPhysicalSchema().getSize(); i++) {
                PhysicalField field = stateDescriptor.getPhysicalSchema().getField(i);
                long inBufferOffset = address + stateDescriptor.getPhysicalSchema().getRecordOffset(i);
                BFType value = field.readValue(new AddressPointer(inBufferOffset));
                object.setValue(i, value);
            }
            return object;
        }

        @Override
        public boolean executeRepeating(VirtualFrame frame) {
            try {
                ScanContext scanContext = (ScanContext) frame.getObject(scanContextSlot);
                BFStateManager stateManager = (BFStateManager) frame.getObject(stateManagerSlot);
                DynHashMap hashMap = (DynHashMap) stateManager.getStateVariable(context.getIndex());

                long currentEntryAddress = hashMap.getEntrySize() * scanContext.current + hashMap.getEntryBuffer();

                long currentValueAddress = DynHashMap.EntryHandler.getValueAddress(currentEntryAddress);

                BFRecord object = getAsLuthObject(currentValueAddress);
                nextOperator.call(object, stateManager);
                scanContext.current++;
                return scanContext.current < scanContext.max;
            } catch (FrameSlotTypeException e) {
                e.printStackTrace();
            }
            return false;
        }
    }

    private class ScanContext {
        private final long max;
        private long current;

        private ScanContext(long max) {
            this.max = max;
        }
    }
}
