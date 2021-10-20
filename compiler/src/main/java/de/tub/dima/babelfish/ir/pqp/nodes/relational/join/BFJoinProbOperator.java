package de.tub.dima.babelfish.ir.pqp.nodes.relational.join;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.records.ReadLuthFieldNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;
import de.tub.dima.babelfish.ir.pqp.objects.state.map.HashMap;
import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;
import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_32;

/**
 * BF Operator for the probe side of a hash join.
 */
@NodeInfo(shortName = "BFJoinProbe")
public class BFJoinProbOperator extends BFOperator {


    @CompilerDirectives.CompilationFinal
    private final JoinContext context;

    @Child
    private ReadLuthFieldNode joinKey;

    @CompilerDirectives.CompilationFinal(dimensions = 1)
    private int[] writeIndex;

    public BFJoinProbOperator(TruffleLanguage<?> language,
                              FrameDescriptor frameDescriptor,
                              BFOperator next,
                              FieldReference key,
                              StateDescriptor stateDescriptor,
                              JoinContext context) {
        super(language, frameDescriptor, next);
        this.joinKey = new ReadLuthFieldNode(key.getKey(), inputObjectSlot);
        this.context = context;
    }

    /**
     * Prepare the result nodes of the join operation.
     *
     * @param inputObject
     */
    public void prepateJoinMerge(BFRecord inputObject) {
        if (writeIndex == null) {
            PhysicalSchema physicalSchema = context.getPhysicalSchema();
            writeIndex = new int[physicalSchema.getSize()];
            for (int i = 0; i < physicalSchema.getSize(); i++) {
                PhysicalField field = physicalSchema.getField(i);
                RecordSchema schema = inputObject.getObjectSchema();
                if (!schema.containsField(field.getName())) {
                    schema.addField(field.getName());
                    writeIndex[i] = schema.getFieldIndex(field.getName());
                }

            }
        }
    }

    @ExplodeLoop
    public BFRecord addJoinState(BFRecord inputObject, long address) {
        PhysicalSchema physicalSchema = context.getPhysicalSchema();

        for (int i = 0; i < physicalSchema.getSize(); i++) {
            PhysicalField field = physicalSchema.getField(i);
            long inBufferOffset = HashMap.EntryHandler.getValueAddress(address) + physicalSchema.getRecordOffset(i);
            BFType value = field.readValue(new AddressPointer(inBufferOffset));
            inputObject.setValue(writeIndex[i], value);
        }
        return inputObject;
    }

    @Override
    public void execute(VirtualFrame frame) {

        BFStateManager stateManager = (BFStateManager) frame.getValue(stateManagerSlot);

        // get hash map from state manager
        HashMap map = (HashMap) stateManager.getStateVariable(context.getIndex());

        if (CompilerDirectives.inInterpreter()) {
            prepateJoinMerge((BFRecord) frame.getValue(inputObjectSlot));
        }
        // get join key from current input record.
        Eager_Int_32 key = (Eager_Int_32) joinKey.execute(frame);
        // get entry for the key
        long entry = map.hasEntry(key.asInt());
        if (entry != 0) {
            // if the entry exists, create join pair and push it to the next operator
            BFRecord inputObject = (BFRecord) frame.getValue(inputObjectSlot);
            addJoinState(inputObject, entry);
            callNextExecute(inputObject, stateManager);
        }
    }
}
