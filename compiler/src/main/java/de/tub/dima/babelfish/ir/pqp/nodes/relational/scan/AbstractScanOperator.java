package de.tub.dima.babelfish.ir.pqp.nodes.relational.scan;


import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.TruffleRuntime;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.nodes.*;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperator;
import de.tub.dima.babelfish.ir.pqp.nodes.BFOperatorInterface;

/**
 * Abstract scan operator for babelfish data formats.
 */
@NodeInfo(shortName = "scan")
public abstract class AbstractScanOperator extends Node implements BFOperatorInterface {

    protected final FrameSlot readerSlot;
    protected final FrameSlot counter;

    protected final FrameDescriptor frameDescriptor;
    protected final String tableName;

    @Child
    protected LoopNode scanRepeatingNode;

    @Child
    protected DirectCallNode openCallNode;

    @Child
    protected DirectCallNode closeCallNode;


    public AbstractScanOperator(TruffleLanguage<?> language,
                                FrameDescriptor frameDescriptor,
                                String layout,
                                BFOperator child) {
        this.tableName = layout;
        this.frameDescriptor = frameDescriptor;
        this.readerSlot = frameDescriptor.findOrAddFrameSlot("reader", FrameSlotKind.Object);
        this.counter = frameDescriptor.findOrAddFrameSlot(STATE_MANAGER_FRAME_SLOT_IDENTIFIER, FrameSlotKind.Object);
        this.scanRepeatingNode = Truffle.getRuntime().createLoopNode(getScanBody(child));
        TruffleRuntime runtime = Truffle.getRuntime();
        this.openCallNode = runtime.createDirectCallNode(
                runtime.createCallTarget(
                        child.getOpenCall()));

        this.closeCallNode = runtime.createDirectCallNode(
                runtime.createCallTarget(
                        child.getCloseCall()));

    }

    protected abstract RepeatingNode getScanBody(BFOperator child);

}
