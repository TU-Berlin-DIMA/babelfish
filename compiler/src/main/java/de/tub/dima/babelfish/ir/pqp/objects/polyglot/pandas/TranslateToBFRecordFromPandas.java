package de.tub.dima.babelfish.ir.pqp.objects.polyglot.pandas;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.Node;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.relational.selection.PredicateNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.typesytem.BFType;

/**
 * This class exposes pandas data frames on top of BFRecords
 */
public class TranslateToBFRecordFromPandas extends Node {

    private final FrameSlot inputObjectSlot;
    @CompilerDirectives.CompilationFinal
    private RecordSchema resultRecordSchema;

    private final FrameDescriptor frameDescriptor = new FrameDescriptor();
    @Children
    private BFExecutableNode[] resultFieldAccesses = new BFExecutableNode[30];

    TranslateToBFRecordFromPandas() {
        inputObjectSlot = frameDescriptor.findOrAddFrameSlot("object", FrameSlotKind.Object);
    }

    public void createResultSchema(PandasDataframeWrapper df,
                                   BFRecord sourceRecord) {
        resultRecordSchema = new RecordSchema();

        RecordSchema inputSchema = sourceRecord.getObjectSchema();
        for (int i = 0; i < inputSchema.getSize(); i++) {
            String inputField = inputSchema.fieldNames[i];
            if (!df.isSelected(inputField))
                continue;
            resultRecordSchema.addField(inputField);
            if (df.isFieldModified(inputField)) {
                PandasExpression modifyField = df.getModifiedField(inputField);
                BFExecutableNode predicate = modifyField.createPredicateNode(frameDescriptor, null);
                resultFieldAccesses[resultRecordSchema.getSize() - 1] = predicate;
            } else {
                BFExecutableNode readNode = new PredicateNode.ReadFieldNode(inputField, frameDescriptor, null);
                resultFieldAccesses[resultRecordSchema.getSize() - 1] = readNode;
            }
        }

        for (String modifiedFields : df.modifiedField.keySet()) {
            if (!resultRecordSchema.containsField(modifiedFields) && df.isSelected(modifiedFields)) {
                resultRecordSchema.addField(modifiedFields);
                BFExecutableNode predicate = df.getModifiedField(modifiedFields).createPredicateNode(frameDescriptor, null);
                resultFieldAccesses[resultRecordSchema.getSize() - 1] = predicate;
            }
        }
    }

    @ExplodeLoop
    public BFRecord createResultRecord(BFRecord sourceRecord) {
        BFRecord resultRecord = BFRecord.createObject(resultRecordSchema);
        for (int i = 0; i < resultRecordSchema.getSize(); i++) {
            VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(new Object[0], frameDescriptor);
            frame.setObject(inputObjectSlot, sourceRecord);
            Object result = resultFieldAccesses[i].execute(frame);
            resultRecord.setValue(i, (BFType) result);
        }
        return resultRecord;
    }

    public BFRecord translate(PandasDataframeWrapper.ExecuteNextOperator df,
                              BFRecord sourceRecord) {
        if (CompilerDirectives.inInterpreter() && resultRecordSchema == null) {
            createResultSchema(df.parent, sourceRecord);
        }
        return createResultRecord(sourceRecord);
    }


}
