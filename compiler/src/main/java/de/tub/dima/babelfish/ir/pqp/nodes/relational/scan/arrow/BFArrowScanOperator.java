package de.tub.dima.babelfish.ir.pqp.nodes.relational.scan.arrow;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.TruffleRuntime;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.*;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperatorInterface;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.typesytem.BFType;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * Scan operator for arrow data
 */
@NodeInfo(shortName = "ArrowScan")
public class BFArrowScanOperator extends Node implements BFOperatorInterface {
    private final FrameSlot readerSlot;

    private final FrameSlot counter;

    @CompilerDirectives.CompilationFinal
    private final String tableName;
    private final FrameDescriptor frameDescriptor;
    @Node.Child
    private LoopNode scanRepeatingNode;
    @Child
    private DirectCallNode openCallNode;
    @Child
    private DirectCallNode closeCallNode;

    public BFArrowScanOperator(TruffleLanguage<?> language,
                               FrameDescriptor frameDescriptor,
                               String layout,
                               BFOperator child) {
        this.frameDescriptor = frameDescriptor;
        this.readerSlot = frameDescriptor.findOrAddFrameSlot("reader", FrameSlotKind.Object);
        this.counter = frameDescriptor.findOrAddFrameSlot(STATE_MANAGER_FRAME_SLOT_IDENTIFIER, FrameSlotKind.Object);
        Catalog catalog = Catalog.getInstance();
        //Schema schema = ((PhysicalCSVLayout) catalog.getLayout(layout)).schema;
        VectorSchemaRoot schema = ((VectorSchemaRoot) catalog.getArrow(layout));

        this.tableName = layout;
        this.scanRepeatingNode = Truffle.getRuntime().createLoopNode(new ScanRepeatingNode(frameDescriptor, child, schema));

        TruffleRuntime runtime = Truffle.getRuntime();
        this.openCallNode = runtime.createDirectCallNode(
                runtime.createCallTarget(
                        child.getOpenCall()));

        this.closeCallNode = runtime.createDirectCallNode(
                runtime.createCallTarget(
                        child.getCloseCall()));
    }

    @CompilerDirectives.TruffleBoundary(transferToInterpreterOnException = false)
    public Buffer getBuffer(String layout) {
        return Catalog.getInstance().getBuffer(layout);
    }

    @Override
    public void open(VirtualFrame frame) {
        openCallNode.call(frame.getValue(counter));
    }

    public void execute(VirtualFrame globalFrame) {

        VirtualFrame scanFrame = Truffle.getRuntime().createVirtualFrame(globalFrame.getArguments(), frameDescriptor);
        scanFrame.setObject(counter, globalFrame.getValue(counter));

        ScanContext context = new ScanContext();
        if (CompilerDirectives.inInterpreter())
            System.out.println(" Start scan of Arrow " + tableName + " from  " + context.currentIndex);
        scanFrame.setObject(readerSlot, context);
        scanRepeatingNode.execute(scanFrame);
    }

    @Override
    public void close(VirtualFrame frame) {
        closeCallNode.call(frame.getValue(counter));
    }

    private final class ScanContext {
        int currentIndex = 0;

        private ScanContext() {
        }
    }

    public class ScanRepeatingNode extends Node implements RepeatingNode {

        @CompilerDirectives.CompilationFinal
        private final FrameSlot objectFrameSlot;

        @CompilerDirectives.CompilationFinal
        private final FrameDescriptor frameDescriptor;

        @CompilerDirectives.CompilationFinal
        private final RecordSchema loopObjectShape;
        @CompilerDirectives.CompilationFinal
        private final int maxRows;
        @Child
        private DirectCallNode next;
        @Children
        private ParseArrowFieldNodesFactory.ParseFieldNode[] parseFieldNodes;

        ScanRepeatingNode(FrameDescriptor frameDescriptor, BFOperator child, VectorSchemaRoot schema) {
            TruffleRuntime runtime = Truffle.getRuntime();
            this.next = runtime.createDirectCallNode(
                    runtime.createCallTarget(
                            child.getExecuteCall()));

            this.frameDescriptor = frameDescriptor;
            this.objectFrameSlot = frameDescriptor.findOrAddFrameSlot("object");
            this.loopObjectShape = new RecordSchema();

            List<FieldVector> fields = schema.getFieldVectors();
            maxRows = schema.getRowCount();
            this.parseFieldNodes = new ParseArrowFieldNodesFactory.ParseFieldNode[fields.size()];
            for (int i = 0; i < parseFieldNodes.length; i++) {
                FieldVector field = fields.get(i);
                this.parseFieldNodes[i] = ParseArrowFieldNodesFactory.createNode(field);
                loopObjectShape.addField(field.getName());
            }
        }

        @Override
        public boolean executeRepeating(VirtualFrame globalFrame) {
            ScanContext context = (ScanContext) globalFrame.getValue(readerSlot);
            if (context.currentIndex < maxRows) {
                BFRecord record = BFRecord.createObject(loopObjectShape);
                readNext(context, record);
                next.call(record, globalFrame.getValue(counter));
                return true;
            }
            return false;
        }

        @ExplodeLoop
        public void readNext(ScanContext context, BFRecord object) {
            for (int i = 0; i < parseFieldNodes.length; i++) {
                VirtualFrame localFrame = Truffle.getRuntime().createVirtualFrame(
                        new Object[]{context.currentIndex}, frameDescriptor);
                BFType value = (BFType) parseFieldNodes[i].execute(localFrame);
                object.setValue(i, value);
            }
            context.currentIndex++;
        }
    }

}
