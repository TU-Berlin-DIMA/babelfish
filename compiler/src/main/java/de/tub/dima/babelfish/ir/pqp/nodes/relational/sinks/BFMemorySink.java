package de.tub.dima.babelfish.ir.pqp.nodes.relational.sinks;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.records.ReadLuthFieldNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.storage.layout.PhysicalLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalRowLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;
import de.tub.dima.babelfish.storage.layout.fields.PhysicalFieldFactory;
import de.tub.dima.babelfish.typesytem.BFType;

/**
 * BF Sink that writes records to a pre-allocated result buffer.
 */
@NodeInfo(shortName = "print")
public class BFMemorySink extends BFOperator {
    private final long pointer;
    @CompilerDirectives.CompilationFinal
    private PhysicalLayout physicalLayout;
    @CompilerDirectives.CompilationFinal
    private RecordSchema schema;
    @Children
    private WriteLuthTypeNode[] writeNodes;

    public BFMemorySink(TruffleLanguage<?> language,
                        FrameDescriptor frameDescriptor) {
        super(language, frameDescriptor);
        pointer = UnsafeUtils.UNSAFE.allocateMemory(100);
    }

    @Override
    public void execute(VirtualFrame localFrame) {
        if (CompilerDirectives.inInterpreter() && writeNodes == null) {
            BFRecord value = (BFRecord) localFrame.getValue(inputObjectSlot);
            schema = value.getObjectSchema();
            computePhysicalLayout(value);

            WriteLuthTypeNode[] printNodes = new WriteLuthTypeNode[schema.last];
            for (int i = 0; i < schema.last; i++) {
                String name = schema.fieldNames[i];
                ReadLuthFieldNode readNode = new ReadLuthFieldNode(name, inputObjectSlot);
                ArgumentReadNode attributeReadNode = new ArgumentReadNode(0);
                printNodes[i] = WriteLuthTypeNode.createNode(readNode, attributeReadNode);
            }
            this.writeNodes = insert(printNodes);
        }

        writeAllFields(localFrame);
    }

    private void computePhysicalLayout(BFRecord object) {
        RecordSchema schema = object.getObjectSchema();
        PhysicalSchema.Builder builder = new PhysicalSchema.Builder();
        for (int i = 0; i < schema.last; i++) {
            String fieldName = schema.fieldNames[i];
            BFType value = object.getValue(fieldName);
            PhysicalField physicalField = PhysicalFieldFactory.getPhysicalField(value, fieldName);
            builder.addField(physicalField);
        }
        physicalLayout = new PhysicalRowLayout(builder.build());
    }

    @ExplodeLoop
    private void writeAllFields(VirtualFrame localFrame) {
        for (int i = 0; i < writeNodes.length; i++) {
            long address = pointer + physicalLayout.getFieldBufferOffset(0, i).getAddress();
            VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(new Object[]{address}, frameDescriptor);
            frame.setObject(inputObjectSlot, localFrame.getValue(inputObjectSlot));
            writeNodes[i].execute(frame);
        }
    }
}
