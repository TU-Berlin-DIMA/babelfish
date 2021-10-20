package de.tub.dima.babelfish.ir.pqp.nodes.polyglot;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.*;
import com.oracle.truffle.api.library.LibraryFactory;
import com.oracle.truffle.api.nodes.DirectCallNode;
import com.oracle.truffle.api.nodes.Node;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.lqp.udf.PolyglotUDFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperatorInterface;
import de.tub.dima.babelfish.ir.pqp.nodes.records.LuthPolyglotBuiltins;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ConstantLuthExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ReadFrameSlotNode;
import de.tub.dima.babelfish.ir.pqp.objects.polyglot.pandas.PandasDataframeLibrary;
import de.tub.dima.babelfish.ir.pqp.objects.polyglot.pandas.PandasDataframeWrapper;
import de.tub.dima.babelfish.ir.pqp.objects.polyglot.pandas.PandasWrapper;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_32;

/**
 * A scalar polyglot UDF operator for Pythons.
 * This special purpose operator intercepts calls to native python libs like numpy.
 */
public class BFScalarUDFOperatorForPythonPandas extends Node implements BFOperatorInterface {
    private final FrameSlot stateManagerSlot;
    private final RecordSchema recordSchema;
    @Child
    private DirectCallNode nextExecute;

    @Child
    private PandasDataframeLibrary dataframeLibrary;

    @Child
    private BFOperator next;

    @Child
    private InteropLibrary interopLibrary;
    protected final FrameSlot polyglotResult;
    protected final FrameSlot operatorResult;
    protected final FrameDescriptor frameDescriptor;

    @CompilerDirectives.CompilationFinal
    protected final FrameSlot polyglotFunctionObjectSlot;

    @CompilerDirectives.CompilationFinal
    protected TruffleObject polyglotFunctionObject;
    @Child
    protected BFExecutableNode evalNode;
    @Child
    @CompilerDirectives.CompilationFinal
    protected BFExecutableNode execNode;

    public BFScalarUDFOperatorForPythonPandas(TruffleLanguage<?> language,
                                              FrameDescriptor frameDescriptor,
                                              BabelfishEngine.BabelfishContext ctx,
                                              PolyglotUDFOperator udf,
                                              BFOperator next) {

        this.frameDescriptor = frameDescriptor;
        polyglotFunctionObjectSlot = frameDescriptor.findOrAddFrameSlot("functionObject", FrameSlotKind.Object);
        polyglotResult = frameDescriptor.findOrAddFrameSlot("polyglotResult", FrameSlotKind.Object);
        operatorResult = frameDescriptor.findOrAddFrameSlot("operatorResult", FrameSlotKind.Object);

        evalNode = LuthPolyglotBuiltins.createEvalNode(ctx, new BFExecutableNode[]{
                new ConstantLuthExecutableNode(udf.getUdf().getLanguage()),
                new ConstantLuthExecutableNode(udf.getUdf().getCode())});

        execNode = LuthPolyglotBuiltins.createExecDirectNode(ctx, new BFExecutableNode[]{
                new ReadFrameSlotNode(polyglotFunctionObjectSlot)
        });

        LibraryFactory<InteropLibrary> INTEROP_LIBRARY_ = LibraryFactory.resolve(InteropLibrary.class);
        interopLibrary = INTEROP_LIBRARY_.createDispatched(30);

        LibraryFactory<PandasDataframeLibrary> DATAFRAME_LIBRARY_ = LibraryFactory.resolve(PandasDataframeLibrary.class);
        dataframeLibrary = DATAFRAME_LIBRARY_.createDispatched(30);

        this.next = next;
        this.stateManagerSlot = frameDescriptor.findOrAddFrameSlot(STATE_MANAGER_FRAME_SLOT_IDENTIFIER);

        this.nextExecute = Truffle.getRuntime().createDirectCallNode(Truffle.getRuntime().createCallTarget(
                next.getExecuteCall())
        );
        recordSchema = new RecordSchema();

    }

    @Override
    public void open(VirtualFrame frame) {
        if (CompilerDirectives.inInterpreter() && polyglotFunctionObject == null) {
            Object returnValue = evalNode.execute(frame);
            polyglotFunctionObject = (TruffleObject) returnValue;
            try {
                Object globalScope = interopLibrary.readMember(this.polyglotFunctionObject, "__globals__");
                Object numpyWrapper = new PandasWrapper(null);
                interopLibrary.writeMember(globalScope, "pd", numpyWrapper);
            } catch (UnsupportedMessageException | UnknownIdentifierException | UnsupportedTypeException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void execute(VirtualFrame frame) {
        frame.setObject(polyglotFunctionObjectSlot, polyglotFunctionObject);
        Object result = execNode.execute(frame);
        if (result instanceof PandasDataframeWrapper) {
            PandasDataframeWrapper res = (PandasDataframeWrapper) execNode.execute(frame);
            PandasDataframeWrapper.ExecuteNextOperator pandasFrame = new PandasDataframeWrapper.ExecuteNextOperator(res, next, (BFStateManager) frame.getValue(stateManagerSlot));
            res.child = pandasFrame;
            dataframeLibrary.open(pandasFrame);
        } else if (result instanceof Integer){
            if(recordSchema.getSize() == 0){
                recordSchema.addField("value");
            }
            BFRecord record = BFRecord.createObject(recordSchema);
            record.setValue(0, new Eager_Int_32((Integer) result));
            nextExecute.call(record, frame.getValue(stateManagerSlot));
        }

    }

    @Override
    public void close(VirtualFrame frame) {

    }
}
