package de.tub.dima.babelfish.ir.pqp.nodes.relational.map;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.typesytem.BFType;

/**
 * BF Operator for map operations.
 * Evaluates a function expression on the input and writes the result in the output.
 */
@NodeInfo(shortName = "MapNode")
public class BFMapOperator extends BFOperator {

    private final String as;
    @CompilerDirectives.CompilationFinal
    RecordSchema resultRecordSchema = new RecordSchema();
    @Child
    private ExpressionNode function;
    @CompilerDirectives.CompilationFinal()
    private int mapFieldIndex;
    @CompilerDirectives.CompilationFinal()
    private int copyFieldIndexes = -1;


    public BFMapOperator(TruffleLanguage<?> language,
                         FrameDescriptor frameDescriptor,
                         BFOperator next,
                         ExpressionNode function,
                         String as) {
        super(language, frameDescriptor, next);
        this.function = function;
        this.as = as;
    }

    @ExplodeLoop
    public BFRecord copyFields(BFRecord inputObject) {

        // Based on the first record we infer the output schema
        if (CompilerDirectives.inInterpreter() && copyFieldIndexes == -1) {
            resultRecordSchema = inputObject.getObjectSchema().copy();
            copyFieldIndexes = resultRecordSchema.last;
            resultRecordSchema.addField(as);
            mapFieldIndex = resultRecordSchema.getFieldIndex(as);
        }

        BFRecord resultObject = BFRecord.createObject(resultRecordSchema);

        for (int i = 0; i < copyFieldIndexes; i++) {
            BFType value = inputObject.getValue(i);
            resultObject.setValue(i, value);
        }

        return resultObject;

    }

    @Override
    public void execute(VirtualFrame frame) {
        BFRecord inputObject = (BFRecord) frame.getValue(inputObjectSlot);
        Object expressionResult = function.execute(frame);
        BFRecord resultObject = copyFields(inputObject);
        resultObject.setValue(mapFieldIndex, (BFType) expressionResult);
        callNextExecute(resultObject, frame.getValue(stateManagerSlot));
    }
}