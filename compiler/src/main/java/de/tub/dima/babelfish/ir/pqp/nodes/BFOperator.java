package de.tub.dima.babelfish.ir.pqp.nodes;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.TruffleRuntime;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.*;

/**
 * Abstract class for all BabelfishEngine operators.
 * Defines default implementation for open, and close as well as some utility functions.
 */
public abstract class BFOperator extends Node implements BFOperatorInterface {

    @CompilerDirectives.CompilationFinal
    public final FrameSlot stateManagerSlot;
    protected final FrameDescriptor frameDescriptor;

    @CompilerDirectives.CompilationFinal
    protected final FrameSlot inputObjectSlot;
    private final TruffleLanguage<?> language;
    @Child
    public DirectCallNode nextOpen;

    @Child
    public DirectCallNode nextExecute;

    @Child
    public DirectCallNode nextClose;


    public BFOperator(TruffleLanguage<?> language, FrameDescriptor frameDescriptor, BFOperator next) {
        this(language, frameDescriptor);
        setNext(next);
    }

    public BFOperator(TruffleLanguage<?> language, FrameDescriptor frameDescriptor) {
        this.language = language;
        this.frameDescriptor = frameDescriptor;
        this.inputObjectSlot = frameDescriptor.findOrAddFrameSlot("object", FrameSlotKind.Object);
        this.stateManagerSlot = frameDescriptor.findOrAddFrameSlot(STATE_MANAGER_FRAME_SLOT_IDENTIFIER, FrameSlotKind.Object);
    }

    @Override
    public void open(VirtualFrame frame) {
        callNextOpen(frame.getValue(stateManagerSlot));
    }

    @Override
    public void close(VirtualFrame frame) {
        callNextClose(frame.getValue(stateManagerSlot));
    }

    public RootNode getExecuteCall() {
        return new BFExecuteRootNode(language, frameDescriptor);
    }

    public RootNode getOpenCall() {
        return new BFOpenRootNode(language, frameDescriptor);
    }

    public RootNode getCloseCall() {
        return new BFCloseRootNode(language, frameDescriptor);
    }

    public void setNext(BFOperator bfOperator) {
        TruffleRuntime runtime = Truffle.getRuntime();
        this.nextOpen =
                runtime.createDirectCallNode(
                        runtime.createCallTarget(
                                bfOperator.getOpenCall()));
        this.nextExecute = runtime.createDirectCallNode(runtime.createCallTarget(
                bfOperator.getExecuteCall())
        );

        this.nextClose = runtime.createDirectCallNode(runtime.createCallTarget(
                bfOperator.getCloseCall())
        );
    }


    protected void callNextOpen(Object... parms) {
        if (nextOpen != null) {
            nextOpen.call(parms);
        }
    }

    protected void callNextExecute(Object... parms) {
        if (nextExecute != null) {
            nextExecute.call(parms);
        }
    }

    protected void callNextClose(Object... parms) {
        if (nextClose != null) {
            nextClose.call(parms);
        }
    }

    @NodeInfo(shortName = "BFOpenCall", cost = NodeCost.MONOMORPHIC)
    private class BFOpenRootNode extends RootNode {
        @Child
        BFOperator parent;

        protected BFOpenRootNode(TruffleLanguage<?> language, FrameDescriptor frameDescriptor) {
            super(language, frameDescriptor);
            parent = BFOperator.this;
        }

        @Override
        public Object execute(VirtualFrame frame) {
            frame.setObject(stateManagerSlot, frame.getArguments()[0]);
            parent.open(frame);
            return null;
        }
    }

    @NodeInfo(shortName = "BFExecuteCall", cost = NodeCost.MONOMORPHIC)
    private class BFExecuteRootNode extends RootNode {

        @Child
        BFOperator parent;

        protected BFExecuteRootNode(TruffleLanguage<?> language, FrameDescriptor frameDescriptor) {
            super(language, frameDescriptor);
            parent = BFOperator.this;
        }

        @Override
        public Object execute(VirtualFrame frame) {
            frame.setObject(inputObjectSlot, frame.getArguments()[0]);
            frame.setObject(stateManagerSlot, frame.getArguments()[1]);
            parent.execute(frame);
            return null;
        }
    }

    @NodeInfo(shortName = "BFExecuteCall", cost = NodeCost.MONOMORPHIC)
    private class BFCloseRootNode extends RootNode {

        @Child
        BFOperator parent;

        protected BFCloseRootNode(TruffleLanguage<?> language,
                                  FrameDescriptor frameDescriptor) {
            super(language, frameDescriptor);
            parent = BFOperator.this;
        }

        @Override
        public Object execute(VirtualFrame frame) {
            frame.setObject(stateManagerSlot, frame.getArguments()[0]);
            parent.close(frame);
            return null;
        }
    }
}
