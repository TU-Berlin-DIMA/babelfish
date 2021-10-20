package de.tub.dima.babelfish.ir.pqp.nodes;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;

/**
 * The BFQueryRootNode is the root node of a BabelfishEngine query PQP.
 * It coordinates the execution of all pipelines.
 * This is structured in three phases:
 * <p>
 * -- initialization
 * In the first phase BF invokes open on all pipelines, which forward the call to all child operators.
 * <p>
 * -- execution
 * In the second phase BF starts the query execution. This phase may be executed concurrently.
 * <p>
 * -- termination
 * In the third phase BF terminates the query execution and closes all pipelines and operators.
 */
@NodeInfo(language = "LU", description = "The root of a query", shortName = "Query")
public class BFQueryRootNode extends BFRootNode implements BFNodeInterface {

    private final FrameSlot stateManagerFrameSlot;

    @Children
    private BFPipelineRoot[] pipelines;

    public BFQueryRootNode(TruffleLanguage<?> language, FrameDescriptor frameDescriptor, BFPipelineRoot[] pipes) {
        super(language, frameDescriptor);
        this.pipelines = new BFPipelineRoot[pipes.length];
        int i = 0;
        for (BFPipelineRoot pipeline : pipes) {
            pipelines[i] = pipeline;
            i++;
        }
        stateManagerFrameSlot = frameDescriptor.findOrAddFrameSlot(STATE_MANAGER_FRAME_SLOT_IDENTIFIER, FrameSlotKind.Object);
    }

    @ExplodeLoop(kind = ExplodeLoop.LoopExplosionKind.FULL_UNROLL)
    private void openPipelines(VirtualFrame frame) {
        for (BFPipelineRoot callableOperator : pipelines) {
            callableOperator.open(frame);
        }
    }

    @ExplodeLoop(kind = ExplodeLoop.LoopExplosionKind.FULL_UNROLL)
    private void executePipelines(VirtualFrame frame) {
        for (BFPipelineRoot callableOperator : pipelines) {
            long start = System.currentTimeMillis();
            callableOperator.execute(frame);
            long end = System.currentTimeMillis();
            if (CompilerDirectives.inInterpreter()) {
                System.out.println("Done with Pipeline");
                System.out.println("Pipline execution time: " + (end - start));
            }
        }
    }

    @ExplodeLoop(kind = ExplodeLoop.LoopExplosionKind.FULL_UNROLL)
    private void closePipelines(VirtualFrame frame) {
        for (BFPipelineRoot callableOperator : pipelines) {
            callableOperator.close(frame);
        }
    }

    @Override
    public Object execute(VirtualFrame frame) {

        VirtualFrame globalQueryFrame = Truffle
                .getRuntime()
                .createVirtualFrame(frame.getArguments(), getFrameDescriptor());
        BFStateManager manager = new BFStateManager();
        globalQueryFrame.setObject(stateManagerFrameSlot, manager);
        // open call pipelines
        openPipelines(globalQueryFrame);

        long start = System.currentTimeMillis();
        // start execution
        executePipelines(globalQueryFrame);

        long duration = System.currentTimeMillis() - start;
        // terminate all pipelines
        closePipelines(globalQueryFrame);
        return duration;
    }
}
