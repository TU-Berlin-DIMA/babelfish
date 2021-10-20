package de.tub.dima.babelfish.ir.pqp.nodes.polyglot;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.lqp.udf.PolyglotUDFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFRootNode;
import de.tub.dima.babelfish.ir.pqp.objects.BFUDFContext;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;

/**
 * A scalar polyglot UDF operator.
 * This operator evaluated a UDF and passes its return value to the next operator.
 */
public class BFScalarUDFOperator extends BFAbstractPolyglotUDFNode {

    protected final FrameSlot resultFrameSlot;

    @Child
    protected TranslatePolyglotResultNode translatePolyglotResultNode;

    public BFScalarUDFOperator(TruffleLanguage<?> language,
                               FrameDescriptor frameDescriptor,
                               BabelfishEngine.BabelfishContext ctx,
                               PolyglotUDFOperator udf,
                               BFOperator next) {
        super(language, frameDescriptor, ctx, udf, next);
        resultFrameSlot = frameDescriptor.addFrameSlot("udf_result");
        translatePolyglotResultNode = TranslatePolyglotResultNode.
                create(ctx, frameDescriptor, resultFrameSlot, operatorResult);
    }

    /**
     * Invoke the udf via the execute node.
     * The actually udf is captured in the polyglot function udf object.
     *
     * @param frame
     */
    protected Object callScalarUDF(VirtualFrame frame) {
        //frame.setObject(polyglotFunctionObjectSlot, polyglotFunctionObject);
        BFUDFContext udfContext = BFUDFContext.createObject(null, (BFStateManager) frame.getValue(stateManagerSlot));
        //frame.setObject(udfContextSlot, udfContext);
        return callNode.call(frame.getValue(inputObjectSlot), frame.getValue(stateManagerSlot), polyglotFunctionObject, udfContext);
    }

    @Override
    public void execute(VirtualFrame frame) {
        Object udfResult = callScalarUDF(frame);
        //frame.setObject(resultFrameSlot, udfResult);
        //PythonAbstractNativeObject nativeObject = (PythonAbstractNativeObject) udfResult;
       // TruffleObjectNativeWrapper truff = TruffleObjectNativeWrapper.wrap(nativeObject);
        VirtualFrame resultFrame = Truffle.getRuntime().createVirtualFrame(new Object[0], frameDescriptor);
        resultFrame.setObject(resultFrameSlot, udfResult);

        BFRecord object = translatePolyglotResultNode.executeAsLuthObject(resultFrame);
        callNextExecute(object, frame.getArguments()[1]);
    }



}
