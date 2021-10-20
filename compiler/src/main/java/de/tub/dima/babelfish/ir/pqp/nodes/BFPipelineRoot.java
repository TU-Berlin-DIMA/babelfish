package de.tub.dima.babelfish.ir.pqp.nodes;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;

/**
 * Root node of a pipeline.
 * Each pipeline starts with a scan and terminates with a pipeline breaker -> aggregation or sink.
 */
@NodeInfo(language = "LU", description = "The root of all Luth execution trees", shortName = "Pipeline")
public class BFPipelineRoot extends Node {

    @Child
    private BFOperatorInterface rootOperator;

    public BFPipelineRoot(BFOperatorInterface rootOperator) {
        this.rootOperator = rootOperator;
    }

    /**
     * Opens this pipeline, e.g., initiates operator state.
     *
     * @param frame
     */
    public void open(VirtualFrame frame) {
        rootOperator.open(frame);
    }

    /**
     * Starts the execution of this pipeline.
     *
     * @param frame
     */
    public void execute(VirtualFrame frame) {
        rootOperator.execute(frame);
    }

    /**
     * Closes this pipeline.
     *
     * @param frame
     */
    public void close(VirtualFrame frame) {
        rootOperator.close(frame);
    }
}
