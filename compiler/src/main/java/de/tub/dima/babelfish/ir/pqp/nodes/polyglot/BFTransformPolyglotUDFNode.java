package de.tub.dima.babelfish.ir.pqp.nodes.polyglot;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.lqp.udf.PolyglotUDFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.objects.BFUDFContext;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;

/**
 * Polyglot operation for transform UDFs.
 * TransformUDFs have 1 input and n output records.
 * To this end, transform UDFs receive a context objects, which allows the UDF to emit any number of result records:
 * <p>
 * function(input, ctx){
 * for(i= 0; i<10;i++){
 * ctx.emit({"counter", input.f1*i});
 * }
 * }
 */
@NodeInfo(shortName = "BFTransformPolyglotUDFNode")
public class BFTransformPolyglotUDFNode extends BFAbstractPolyglotUDFNode {

    private final BFOperator next;

    public BFTransformPolyglotUDFNode(TruffleLanguage<?> language, FrameDescriptor frameDescriptor,
                                      BabelfishEngine.BabelfishContext ctx,
                                      PolyglotUDFOperator udf,
                                      BFOperator next) {
        super(language, frameDescriptor, ctx, udf, next);
        this.next = next;
    }

    @Override
    public void execute(VirtualFrame frame) {
        BFStateManager bfStateManager = getStateManager(frame);
        // place function scope on the local frame.
        frame.setObject(polyglotFunctionObjectSlot, polyglotFunctionObject);
        BFUDFContext udfContext = BFUDFContext.createObject(next, bfStateManager);
        frame.setObject(udfContextSlot, udfContext);
        execNode.execute(frame);
    }
}
