package de.tub.dima.babelfish.ir.pqp.nodes.relational.scan;


import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.TruffleRuntime;
import com.oracle.truffle.api.frame.*;
import com.oracle.truffle.api.nodes.*;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperatorInterface;
import de.tub.dima.babelfish.ir.pqp.nodes.records.BFGetFieldIndexNode;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.sinks.LuthBufferReader;
import de.tub.dima.babelfish.ir.pqp.nodes.state.InitStateVariableNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;
import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.typesytem.BFType;

@NodeInfo(shortName = "scan")
public class BFChunkedScan extends Node implements BFOperatorInterface {

    @CompilerDirectives.CompilationFinal
    private final LuthBufferReader bufferReader;

    private final FrameDescriptor chunkFrameDescriptor;
    private final FrameSlot chunkContextSlot;
    private final FrameSlot stateManagerSlot;
    private final FrameSlot bufferAddressSlot;
    private final boolean useLocalStateManager;
    @CompilerDirectives.CompilationFinal
    private int variableSlots;
    @Child
    private LoopNode scanRepeatingNode;

    @CompilerDirectives.CompilationFinal(dimensions = 1)
    private StateDescriptor[] valueStateDescriptors;

    @Children
    private InitStateVariableNode[] initStateVariableNode;

    @Child
    private DirectCallNode openCallNode;

    @Child
    private DirectCallNode closeCallNode;


    public BFChunkedScan(TruffleLanguage<?> language,
                         FrameDescriptor frameDescriptor,
                         String tableName,
                         BFOperator child) {
        this.chunkFrameDescriptor = new FrameDescriptor();
        this.chunkContextSlot = chunkFrameDescriptor
                .addFrameSlot("chunkContext", FrameSlotKind.Object);
        this.stateManagerSlot = chunkFrameDescriptor
                .addFrameSlot("stateManager", FrameSlotKind.Object);
        this.bufferAddressSlot = chunkFrameDescriptor
                .addFrameSlot("bufferAddressSlot", FrameSlotKind.Long);
        Catalog catalog = Catalog.getInstance();
        this.bufferReader = new LuthBufferReader(catalog.getLayout(tableName));
        this.scanRepeatingNode = Truffle.getRuntime()
                .createLoopNode(new ChunkedScanRepeatingNode(chunkFrameDescriptor, child));
        useLocalStateManager = true;
        TruffleRuntime runtime = Truffle.getRuntime();
        this.openCallNode = runtime.createDirectCallNode(
                runtime.createCallTarget(
                        child.getOpenCall()));

        this.closeCallNode = runtime.createDirectCallNode(
                runtime.createCallTarget(
                        child.getCloseCall()));

    }

    @ExplodeLoop
    public BFStateManager getLocalStateManager(BFStateManager stateManager) {
        if (!useLocalStateManager) {
            return null;
        }


        BFStateManager localStateManager = new BFStateManager();
          /*  for (int i = 0; i < variableSlots; i++) {
            int index = localStateManager.registerValueState(valueStateDescriptors[i]);
            StateVariable var = localStateManager.get(index);
            initStateVariableNode[i].execute(Truffle.getRuntime().createVirtualFrame(new Object[]{var}, chunkFrameDescriptor));
        }*/
        return localStateManager;
    }

    @Override
    public void open(VirtualFrame frame) {
        openCallNode.call(frame.getValue(stateManagerSlot));
    }


    public void execute(VirtualFrame globalFrame) {
        //long start = System.currentTimeMillis();
        BFStateManager stateManager =
                (BFStateManager) globalFrame.getArguments()[0];
        BFStateManager localStateManager = getLocalStateManager(stateManager);
        long bufferAddress =
                (long) globalFrame.getArguments()[1];
        ParallelScanContext parallelScanContext =
                (ParallelScanContext) globalFrame.getArguments()[2];

        ChunkContext chunk = parallelScanContext.getNextChunkStart();

        int counter = 0;
        try {
            while (chunk.hasMore()) {
                VirtualFrame chunkFrame = Truffle.getRuntime().createVirtualFrame(new Object[0], chunkFrameDescriptor);
                chunkFrame.setObject(chunkContextSlot, chunk);
                chunkFrame.setObject(stateManagerSlot, localStateManager);
                chunkFrame.setLong(bufferAddressSlot, bufferAddress);
                scanRepeatingNode.execute(chunkFrame);
                chunk = parallelScanContext.getNextChunkStart();
                counter++;
            }

        } finally {
            LoopNode.reportLoopCount(this, counter);
        }

        // mergeState(localStateManager, stateManager);

        //  long end = System.currentTimeMillis();

        if (CompilerDirectives.inInterpreter()) {
            System.out.println(Thread.currentThread().getName() + ": Scan done");
        }
    }

    @Override
    public void close(VirtualFrame frame) {
        closeCallNode.call(frame.getValue(stateManagerSlot));
    }
/*
    @ExplodeLoop
    public void mergeState(BFStateManager localStateManager, BFStateManager globalStateManager) {

        if (CompilerDirectives.inInterpreter() && localStateManager.getVariables() == 0) {
            return;
        }
        if (CompilerDirectives.inInterpreter() && localStateManager.getVariables() > 0) {
            synchronized (this) {
                if (variableSlots == 0) {
                    variableSlots = localStateManager.getVariables();
                    valueStateDescriptors = localStateManager.getStateDescriptors();
                    initStateVariableNode = new InitStateVariableNode[variableSlots];
                    for (int i = 0; i < variableSlots; i++) {
                        initStateVariableNode[i] = insert(InitStateVariableNode.create(valueStateDescriptors[i]));
                        globalStateManager.registerValueState(valueStateDescriptors[i]);
                    }
                }
            }
        }


        // merge state
        if (variableSlots >= 0) {
            for (int i = 0; i < variableSlots; i++) {
                StateVariable localStateVariable = localStateManager.get(i);
                StateVariable globalStateVariable = globalStateManager.get(i);
                long localValue = UnsafeUtils.getLong(localStateVariable.getAddress());
                UnsafeUtils.UNSAFE.getAndAddLong(null, globalStateVariable.getAddress(), localValue);
            }
        }


    }
*/

    public class ChunkedScanRepeatingNode extends Node implements RepeatingNode {

        @CompilerDirectives.CompilationFinal
        private final RecordSchema loopObjectShape;

        @Child
        private DirectCallNode next;

        @Children
        private BFGetFieldIndexNode[] getFieldIndexNodes;

        ChunkedScanRepeatingNode(FrameDescriptor frameDescriptor, BFOperator child) {

            TruffleRuntime runtime = Truffle.getRuntime();
            this.next = runtime.createDirectCallNode(
                    runtime.createCallTarget(
                            child.getExecuteCall()));

            this.loopObjectShape = new RecordSchema();
            this.getFieldIndexNodes =
                    new BFGetFieldIndexNode[bufferReader.getPhysicalLayout().getSchema().getSize()];
            for (int i = 0; i < bufferReader.getPhysicalLayout().getSchema().getSize(); i++) {
                String name = bufferReader.getPhysicalLayout().getSchema().getField(i).getName();
                getFieldIndexNodes[i] = new BFGetFieldIndexNode(name);
                loopObjectShape.addField(name);
            }
        }

        @Override
        public boolean executeRepeating(VirtualFrame globalFrame) {

            try {
                ChunkContext chunkContext =
                        (ChunkContext) globalFrame.getObject(chunkContextSlot);
                BFStateManager stateManager =
                        (BFStateManager) globalFrame.getObject(stateManagerSlot);

                long bufferAddress = globalFrame.getLong(bufferAddressSlot);

                if (chunkContext.hasMore()) {
                    BFRecord record = BFRecord.createObject(loopObjectShape);
                    long currentIndex = chunkContext.next();
                    readNext(bufferReader, bufferAddress, currentIndex, record);
                    next.call(record, stateManager);
                    return true;
                }
            } catch (FrameSlotTypeException e) {


            }
            return false;

        }

        @ExplodeLoop
        public void readNext(LuthBufferReader reader, long buffer, long recordIndex, BFRecord object) {

            for (int i = 0; i < reader.getPhysicalLayout().getSchema().getSize(); i++) {
                PhysicalField field = reader.getPhysicalLayout().getSchema().getField(i);
                AddressPointer inBufferOffset = reader.getPhysicalLayout().getFieldBufferOffset(recordIndex, i);
                int index = getFieldIndexNodes[i].getFieldIndex(object.getObjectSchema());
                BFType value = field.readValue(new AddressPointer(buffer + inBufferOffset.getAddress()));
                object.setValue(index, value);
            }
        }
    }
}
