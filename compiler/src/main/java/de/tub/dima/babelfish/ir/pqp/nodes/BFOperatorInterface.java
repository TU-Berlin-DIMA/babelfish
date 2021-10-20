package de.tub.dima.babelfish.ir.pqp.nodes;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInterface;

/**
 * General interface for all BabelfishEngine operators.
 * It defines open, execute, and close.
 */
public interface BFOperatorInterface extends NodeInterface, BFNodeInterface {

    /**
     * Initiates the operator state.
     * Is called before any of the other functions.
     *
     * @param frame
     */
    void open(VirtualFrame frame);

    /**
     * Executes the operator for one input record.
     * Is called multiple times.
     *
     * @param frame
     */
    void execute(VirtualFrame frame);

    /**
     * Terminates this operator.
     * Is called exactly once.
     *
     * @param frame
     */
    void close(VirtualFrame frame);


}
