package de.tub.dima.babelfish.ir.pqp.nodes;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.nodes.DirectCallNode;
import com.oracle.truffle.api.nodes.RootNode;

public abstract class BFRootNode extends RootNode {

    public final FrameSlot stateManagerSlot;

    protected BFRootNode(TruffleLanguage<?> language, FrameDescriptor frameDescriptor) {
        super(language, frameDescriptor);
        this.stateManagerSlot = frameDescriptor.findOrAddFrameSlot("counter", FrameSlotKind.Object);
    }

    public DirectCallNode createCallTarget(BFRootNode nextOperator) {
        CallTarget callTarget = Truffle.getRuntime().createCallTarget(nextOperator);
        return Truffle.getRuntime().createDirectCallNode(callTarget);
    }


}
