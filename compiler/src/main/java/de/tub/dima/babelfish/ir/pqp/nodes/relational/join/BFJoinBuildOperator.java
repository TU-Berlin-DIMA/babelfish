package de.tub.dima.babelfish.ir.pqp.nodes.relational.join;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.lqp.relational.KeyGroup;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.records.ReadLuthFieldNode;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.sinks.WriteLuthTypeNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;
import de.tub.dima.babelfish.ir.pqp.objects.state.map.HashMap;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;
import de.tub.dima.babelfish.storage.layout.fields.PhysicalFieldFactory;
import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

import java.util.ArrayList;

/**
 * BF Operator for the build side of a hash join.
 */
@NodeInfo(shortName = "joinBuild")
public class BFJoinBuildOperator extends BFOperator {

    private final JoinContext context;
    @CompilerDirectives.CompilationFinal
    FrameDescriptor localFrameDescriptor = new FrameDescriptor();
    @Child
    private ReadLuthFieldNode readJoinKeyNode;
    @CompilerDirectives.CompilationFinal
    private PhysicalSchema physicalSchema;
    @Children
    private ReadLuthFieldNode[] readValueFieldNodes;
    @Children
    private WriteLuthTypeNode[] writeLuthFieldNode;

    public BFJoinBuildOperator(TruffleLanguage<?> language,
                               FrameDescriptor frameDescriptor,
                               KeyGroup key,
                               StateDescriptor stateDescriptor,
                               JoinContext context) {
        super(language, frameDescriptor);
        this.context = context;
        this.readJoinKeyNode = new ReadLuthFieldNode(key.getKeys().get(0).getKey(), inputObjectSlot);

    }

    @ExplodeLoop
    public void writeLuthObject(long address, VirtualFrame frame) {
        for (int i = 0; i < physicalSchema.getSize(); i++) {
            long inBufferOffset = address + physicalSchema.getRecordOffset(i);
            BFType value = (BFType) readValueFieldNodes[i].execute(frame);
            VirtualFrame localFrame = Truffle.getRuntime().createVirtualFrame(new Object[]{
                    value, inBufferOffset
            }, localFrameDescriptor);
            writeLuthFieldNode[0].execute(localFrame);
        }
    }


    @Override
    public void open(VirtualFrame frame) {
        super.open(frame);
        BFStateManager stateManager = (BFStateManager) frame.getValue(stateManagerSlot);
        if (physicalSchema != null) {
            HashMap hashMap = new HashMap(context.getCardinality(), physicalSchema.getFixedRecordSize());
            stateManager.setStateVariable(context.getIndex(), hashMap);
        }
    }

    @Override
    public void execute(VirtualFrame frame) {

        BFRecord inputObject = (BFRecord) frame.getValue(inputObjectSlot);
        BFStateManager stateManager = (BFStateManager) frame.getValue(stateManagerSlot);

        if (CompilerDirectives.inInterpreter() && physicalSchema == null) {
            physicalSchema = getPhysicalSchema(inputObject);
            System.out.println("Create state for BFJoinBuildOperator for index: ");
            HashMap hashMap = new HashMap(context.getCardinality(), physicalSchema.getFixedRecordSize());
            stateManager.setStateVariable(context.getIndex(), hashMap);
            context.addPhysicalSchema(physicalSchema);
        }

        HashMap map = (HashMap) stateManager.getStateVariable(context.getIndex());

        // read the join key, currently we assume int 32 join keys.
        Eager_Int_32 key = (Eager_Int_32) readJoinKeyNode.execute(frame);
        long entryAddress = map.findEntry(key.asInt());
        writeLuthObject(entryAddress, frame);
    }

    @Override
    public void close(VirtualFrame frame) {
        super.close(frame);
        BFStateManager stateManager = (BFStateManager) frame.getValue(stateManagerSlot);
        HashMap map = (HashMap) stateManager.getStateVariable(context.getIndex());
        map.free();
    }

    private PhysicalSchema getPhysicalSchema(BFRecord record) {
        CompilerDirectives.transferToInterpreterAndInvalidate();
        PhysicalSchema.Builder physicalSchemaBuilder = new PhysicalSchema.Builder();
        ArrayList<ReadLuthFieldNode> readFields = new ArrayList<>();
        ArrayList<WriteLuthTypeNode> writeValueNodes = new ArrayList<>();
        for (int i = 0; i < record.getObjectSchema().last; i++) {
            String name = record.getObjectSchema().fieldNames[i];
            BFType type = record.getValue(name);
            if (type instanceof Numeric)
                physicalSchemaBuilder.addField(PhysicalFieldFactory.getPhysicalField(type.getClass(), name, 0, ((Numeric) type).getPrecision()));
            else if (type instanceof Text)
                physicalSchemaBuilder.addField(PhysicalFieldFactory.getPhysicalField(type.getClass(), name, ((Text) type).length(), 0));
            else
                physicalSchemaBuilder.addField(PhysicalFieldFactory.getPhysicalField(type.getClass(), name));
            readFields.add(new ReadLuthFieldNode(name, inputObjectSlot));
            writeValueNodes.add(WriteLuthTypeNode.createNode(new ArgumentReadNode(0), new ArgumentReadNode(1)));
        }
        readValueFieldNodes = readFields.toArray(new ReadLuthFieldNode[readFields.size()]);
        writeLuthFieldNode = writeValueNodes.toArray(new WriteLuthTypeNode[writeValueNodes.size()]);
        return physicalSchemaBuilder.build();
    }
}
