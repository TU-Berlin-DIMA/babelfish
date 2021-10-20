package de.tub.dima.babelfish.ir.pqp.nodes.polyglot;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.nodes.DirectCallNode;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.lqp.udf.PolyglotUDFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFRootNode;
import de.tub.dima.babelfish.ir.pqp.nodes.records.LuthPolyglotBuiltins;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ConstantLuthExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ReadFrameSlotNode;
import de.tub.dima.babelfish.ir.pqp.objects.BFUDFContext;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;

/**
 * Abstract operator for all polyglot operators.
 */
@NodeInfo(shortName = "BFAbstractPolyglotUDFNode")
public abstract class BFAbstractPolyglotUDFNode extends BFOperator {

    protected final FrameSlot polyglotResult;
    protected final FrameSlot operatorResult;
    protected final FrameDescriptor frameDescriptor;
    @CompilerDirectives.CompilationFinal
    protected final FrameSlot udfContextSlot;
    @CompilerDirectives.CompilationFinal
    protected final FrameSlot polyglotFunctionObjectSlot;
    @Child
    protected DirectCallNode callNode;
    @Child
    protected BFExecutableNode evalNode;
    @Child
    @CompilerDirectives.CompilationFinal
    protected BFExecutableNode execNode;
    @CompilerDirectives.CompilationFinal
    protected TruffleObject polyglotFunctionObject;


    public BFAbstractPolyglotUDFNode(TruffleLanguage<?> language, FrameDescriptor frameDescriptor,
                                     BabelfishEngine.BabelfishContext ctx,
                                     PolyglotUDFOperator udf,
                                     BFOperator next) {
        super(language, frameDescriptor, next);

        evalNode = LuthPolyglotBuiltins.createEvalNode(ctx, new BFExecutableNode[]{
                new ConstantLuthExecutableNode(udf.getUdf().getLanguage()),
                new ConstantLuthExecutableNode(udf.getUdf().getCode())});

        this.frameDescriptor = frameDescriptor;
        polyglotFunctionObjectSlot = frameDescriptor.findOrAddFrameSlot("functionObject", FrameSlotKind.Object);
        udfContextSlot = frameDescriptor.findOrAddFrameSlot("context", FrameSlotKind.Object);
        polyglotResult = frameDescriptor.findOrAddFrameSlot("polyglotResult", FrameSlotKind.Object);
        operatorResult = frameDescriptor.findOrAddFrameSlot("operatorResult", FrameSlotKind.Object);

        BFExecutableNode execNode = LuthPolyglotBuiltins.createExecNode(ctx, new BFExecutableNode[]{
                new ReadFrameSlotNode(polyglotFunctionObjectSlot),
                new ReadFrameSlotNode(inputObjectSlot),
                new ReadFrameSlotNode(udfContextSlot)
        });

        RootCallTarget callTarget = Truffle.getRuntime().createCallTarget(new UDFInvokeNode(language, frameDescriptor, execNode));

        callNode =  Truffle.getRuntime().createDirectCallNode(callTarget);


    }

    @Override
    public void open(VirtualFrame frame) {
        if (CompilerDirectives.inInterpreter() && polyglotFunctionObject == null) {
            Object returnValue = evalNode.execute(frame);
            polyglotFunctionObject = (TruffleObject) returnValue;
        }
        super.open(frame);
    }

    protected BFStateManager getStateManager(VirtualFrame frame) {
        return (BFStateManager) frame.getValue(stateManagerSlot);
    }

    public class UDFInvokeNode extends BFRootNode {

        @Child
        private  BFExecutableNode execNode;

        protected UDFInvokeNode(TruffleLanguage<?> language, FrameDescriptor frameDescriptor, BFExecutableNode execNode) {
            super(language, frameDescriptor);
            this.execNode = execNode;

        }


        @Override
        public Object execute(VirtualFrame frame) {

            frame.setObject(inputObjectSlot, frame.getArguments()[0]);
            frame.setObject(stateManagerSlot, frame.getArguments()[1]);
            frame.setObject(polyglotFunctionObjectSlot, frame.getArguments()[2]);
            frame.setObject(udfContextSlot, frame.getArguments()[3]);
            return execNode.execute(frame);
        }

        @Override
        public boolean isCloningAllowed() {
            return true;
        }

        @Override
        public Node copy() {
            return new UDFInvokeNode(null, frameDescriptor, execNode);
        }

        @Override
        protected boolean isCloneUninitializedSupported() {
            return true;
        }

        @Override
        public boolean isAdoptable() {
            return super.isAdoptable();
        }
    }
}
