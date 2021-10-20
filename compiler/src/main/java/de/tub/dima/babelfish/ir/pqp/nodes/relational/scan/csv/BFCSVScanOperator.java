package de.tub.dima.babelfish.ir.pqp.nodes.relational.scan.csv;


import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.TruffleRuntime;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.*;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.scan.AbstractScanOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.storage.layout.PhysicalCSVLayout;
import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.schema.Schema;
import de.tub.dima.babelfish.typesytem.schema.field.SchemaField;

/**
 * BF scan operator for CSV data
 */
@NodeInfo(shortName = "CSVScan")
public class BFCSVScanOperator extends AbstractScanOperator {

    private final FrameSlot readerSlot;

    private final FrameSlot counter;

    @CompilerDirectives.CompilationFinal
    private Schema schema;

    public BFCSVScanOperator(TruffleLanguage<?> language,
                             FrameDescriptor frameDescriptor,
                             String layout,
                             BFOperator child) {
        super(language, frameDescriptor, layout, child);
        this.readerSlot = frameDescriptor.findOrAddFrameSlot("reader", FrameSlotKind.Object);
        this.counter = frameDescriptor.findOrAddFrameSlot(STATE_MANAGER_FRAME_SLOT_IDENTIFIER, FrameSlotKind.Object);
    }

    public RepeatingNode getScanBody(BFOperator child) {
        Catalog catalog = Catalog.getInstance();
        schema = ((PhysicalCSVLayout) catalog.getLayout(tableName)).schema;
        return new ScanRepeatingNode(frameDescriptor, child, schema);
    }

    @Override
    public void open(VirtualFrame frame) {
        openCallNode.call(frame.getValue(counter));
    }

    public void execute(VirtualFrame globalFrame) {
        VirtualFrame scanFrame = Truffle.getRuntime().createVirtualFrame(globalFrame.getArguments(), frameDescriptor);
        scanFrame.setObject(counter, globalFrame.getValue(counter));
        Buffer buffer = getBuffer(tableName);
        ScanContext context = new ScanContext(buffer.getVirtualAddress().getAddress(), buffer.getVirtualAddress().getAddress() + buffer.getSize().getBytes());
        if (CompilerDirectives.inInterpreter())
            System.out.println(" Start scan of CSV " + tableName + " from  " + context.currentAddres + " to" + context.endAddress);
        scanFrame.setObject(readerSlot, context);
        scanRepeatingNode.execute(scanFrame);
    }

    @Override
    public void close(VirtualFrame frame) {
        closeCallNode.call(frame.getValue(counter));
    }


    @CompilerDirectives.TruffleBoundary(transferToInterpreterOnException = false)
    public Buffer getBuffer(String layout) {
        return Catalog.getInstance().getBuffer(layout);
    }

    @NodeChild(type = ArgumentReadNode.class, value = "currentPosition")
    @NodeChild(type = ArgumentReadNode.class, value = "endPosition")
    public static abstract class SkipFieldNode extends Node {
        abstract long execute(VirtualFrame frame);

        boolean isSep(long endPosition, long position, long size) {
            long speculatedPosition = position + size;
            return UnsafeUtils.getByte(speculatedPosition) == '|';
        }

        long calcSize(long currentPosition) {
            return skip(currentPosition, 0) - currentPosition;
        }

        // @Specialization(guards = {"isSep(endPosition, currentPosition, size)"}, limit = "2")
        //  long skipWithSize(long currentPosition, long endPosition, @Cached(value = "calcSize(currentPosition)") long size) {
        //    return currentPosition + size;
        // }

        @Specialization()
        long skip(long currentPosition, long endPosition) {
            while (UnsafeUtils.getByte(currentPosition) != '|') {
                currentPosition++;
            }
            return currentPosition;
        }


    }

    private final class ScanContext {
        final long endAddress;
        long currentAddres = 0;

        private ScanContext(long startAddress, long endAddress) {
            this.endAddress = endAddress;
            this.currentAddres = startAddress;
        }
    }

    public class ScanRepeatingNode extends Node implements RepeatingNode {

        @CompilerDirectives.CompilationFinal
        private final FrameSlot objectFrameSlot;

        @CompilerDirectives.CompilationFinal
        private final FrameDescriptor frameDescriptor;

        @CompilerDirectives.CompilationFinal
        private final RecordSchema loopObjectShape;

        @Child
        private DirectCallNode next;

        @Children
        private ParseCSVFieldNodesFactory.ParseFieldNode[] parseFieldNodes;

        @Children
        private SkipFieldNode[] skipFieldNodes;


        ScanRepeatingNode(FrameDescriptor frameDescriptor, BFOperator child, Schema schema) {
            TruffleRuntime runtime = Truffle.getRuntime();
            this.next = runtime.createDirectCallNode(
                    runtime.createCallTarget(
                            child.getExecuteCall()));

            this.frameDescriptor = frameDescriptor;
            this.objectFrameSlot = frameDescriptor.findOrAddFrameSlot("object");
            this.loopObjectShape = new RecordSchema();
            this.parseFieldNodes = new ParseCSVFieldNodesFactory.ParseFieldNode[schema.getFields().length];
            this.skipFieldNodes = new SkipFieldNode[schema.getFields().length];
            for (int i = 0; i < parseFieldNodes.length; i++) {
                SchemaField field = schema.getFields()[i];
                parseFieldNodes[i] = ParseCSVFieldNodesFactory.createNode(field);
                skipFieldNodes[i] = BFCSVScanOperatorFactory.SkipFieldNodeGen.create(new ArgumentReadNode(0), new ArgumentReadNode(1));
                loopObjectShape.addField(field.getName());
            }
        }

        @Override
        public boolean executeRepeating(VirtualFrame globalFrame) {
            ScanContext context = (ScanContext) globalFrame.getValue(readerSlot);
            if (context.currentAddres < context.endAddress) {

                BFRecord record = BFRecord.createObject(loopObjectShape);

                readNext(context, record);
                next.call(record, globalFrame.getValue(counter));
                return true;
            }
            return false;
        }

        public long getNextFieldEndPosition(long currentPosition) {
            while (UnsafeUtils.getByte(currentPosition) != '|') {
                currentPosition++;
            }
            return currentPosition;
        }

        @ExplodeLoop
        public void readNext(ScanContext context, BFRecord object) {

            for (int i = 0; i < parseFieldNodes.length; i++) {
                long fieldStartPosition = context.currentAddres;
                VirtualFrame potionFrame = Truffle.getRuntime().createVirtualFrame(
                        new Object[]{fieldStartPosition, context.endAddress}, frameDescriptor);
                long fieldEndPosition = skipFieldNodes[i].execute(potionFrame);
                //if(fieldEndPosition != getNextFieldEndPosition(fieldStartPosition)){
                //   throw new RuntimeException("huch");
                // }
                //System.out.println("Parse field - " + fieldStartPosition + " - " + fieldEndPosition);
                VirtualFrame localFrame = Truffle.getRuntime().createVirtualFrame(
                        new Object[]{fieldStartPosition, fieldEndPosition}, frameDescriptor);
                BFType value = (BFType) parseFieldNodes[i].execute(localFrame);
                object.setValue(i, value);
                // if((char)UnsafeUtils.getByte(fieldEndPosition) != '|')
                //     throw new RuntimeException("huch");
                context.currentAddres = fieldEndPosition + 1;
            }
            context.currentAddres = context.currentAddres + 1;
        }
    }


}
