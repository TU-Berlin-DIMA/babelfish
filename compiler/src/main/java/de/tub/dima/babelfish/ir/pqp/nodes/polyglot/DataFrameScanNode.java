package de.tub.dima.babelfish.ir.pqp.nodes.polyglot;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleRuntime;
import com.oracle.truffle.api.frame.*;
import com.oracle.truffle.api.library.LibraryFactory;
import com.oracle.truffle.api.nodes.*;
import de.tub.dima.babelfish.ir.pqp.nodes.records.BFGetFieldIndexNode;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.sinks.LuthBufferReader;
import de.tub.dima.babelfish.ir.pqp.objects.polyglot.pandas.PandasDataframeLibrary;
import de.tub.dima.babelfish.ir.pqp.objects.polyglot.pandas.PandasDataframeWrapper;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.typesytem.BFType;

@NodeInfo
public class DataFrameScanNode extends Node {
    private final String tableName;
    private final LuthBufferReader bufferReader;
    private final FrameDescriptor frameDescriptor;
    private final FrameSlot readerSlot;
    private final FrameSlot childSlot;
    @Child
    private LoopNode scanRepeatingNode;

    public DataFrameScanNode(String tableName) {
        this.tableName = tableName;
        Catalog catalog = Catalog.getInstance();
        this.bufferReader = new LuthBufferReader(catalog.getLayout(tableName));
        this.frameDescriptor = new FrameDescriptor();
        this.readerSlot = frameDescriptor.findOrAddFrameSlot("reader", FrameSlotKind.Object);
        this.childSlot = frameDescriptor.findOrAddFrameSlot("child", FrameSlotKind.Object);
        this.scanRepeatingNode = Truffle.getRuntime().createLoopNode(new ScanRepeatingNode(frameDescriptor));
    }

    @CompilerDirectives.TruffleBoundary(transferToInterpreterOnException = false)
    public long getBuffer(String layout) {
        return Catalog.getInstance().getBuffer(layout).getVirtualAddress().getAddress();
    }

    public void open(PandasDataframeWrapper child) {
        long buffer = getBuffer(tableName);
        LuthBufferReader.ReaderContext context = bufferReader.createContext(buffer);
        if (CompilerDirectives.inInterpreter())
            System.out.println(" Start scan of " + tableName + " with " + context.getMaxNumberOfRecord() + " records");

        VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(new Object[0], frameDescriptor);
        frame.setObject(readerSlot, context);
        frame.setObject(childSlot, child);
        scanRepeatingNode.execute(frame);
    }

    public class ScanRepeatingNode extends Node implements RepeatingNode {

        private final FrameDescriptor frameDescriptor;
        private final FrameSlot objectFrameSlot;
        private final RecordSchema loopObjectShape;

        @Children
        private BFGetFieldIndexNode[] getFieldIndexNodes;
        @Child
        private PandasDataframeLibrary pandasLib;

        ScanRepeatingNode(FrameDescriptor frameDescriptor) {
            TruffleRuntime runtime = Truffle.getRuntime();
            this.frameDescriptor = frameDescriptor;
            LibraryFactory<PandasDataframeLibrary> INTEROP_LIBRARY_ = LibraryFactory.resolve(PandasDataframeLibrary.class);
            pandasLib = INTEROP_LIBRARY_.createDispatched(30);
            this.objectFrameSlot = frameDescriptor.findOrAddFrameSlot("object");
            this.loopObjectShape = new RecordSchema();
            getFieldIndexNodes = new BFGetFieldIndexNode[bufferReader.getPhysicalLayout().getSchema().getSize()];
            for (int i = 0; i < bufferReader.getPhysicalLayout().getSchema().getSize(); i++) {
                String name = bufferReader.getPhysicalLayout().getSchema().getField(i).getName();
                getFieldIndexNodes[i] = new BFGetFieldIndexNode(name);
                loopObjectShape.addField(name);
            }
        }

        @Override
        public boolean executeRepeating(VirtualFrame globalFrame) {
            LuthBufferReader.ReaderContext context = null;
            try {
                context = (LuthBufferReader.ReaderContext) globalFrame.getObject(readerSlot);

                if (bufferReader.hasNext(context)) {
                    VirtualFrame loopFrame = Truffle.getRuntime().createVirtualFrame(new Object[0], frameDescriptor);

                    BFRecord record = BFRecord.createObject(loopObjectShape);

                    readNext(bufferReader, context, record);
                    bufferReader.next(context, record);
                    loopFrame.setObject(objectFrameSlot, record);

                    pandasLib.execute((PandasDataframeWrapper) globalFrame.getValue(childSlot), record);
                    return true;
                }

            } catch (FrameSlotTypeException e) {
                e.printStackTrace();
            }
            return false;
        }

        @ExplodeLoop
        public void readNext(LuthBufferReader reader, LuthBufferReader.ReaderContext context, BFRecord object) {

            for (int i = 0; i < reader.getPhysicalLayout().getSchema().getSize(); i++) {
                PhysicalField field = reader.getPhysicalLayout().getSchema().getField(i);
                AddressPointer inBufferOffset = reader.getPhysicalLayout().getFieldBufferOffset(context.getCurrentIndex(), i);
                int index = getFieldIndexNodes[i].getFieldIndex(object.getObjectSchema());
                BFType value = field.readValue(new AddressPointer(context.getBuffer() + inBufferOffset.getAddress()));
                object.setValue(index, value);
            }
        }
    }


}
