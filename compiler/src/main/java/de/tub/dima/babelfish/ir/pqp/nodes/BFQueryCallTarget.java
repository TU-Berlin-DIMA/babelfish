package de.tub.dima.babelfish.ir.pqp.nodes;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.nodes.RootNode;
import de.tub.dima.babelfish.ir.pqp.objects.ExecutableQuery;

@NodeInfo(language = "LU", description = "The root of all Luth execution trees")
public class BFQueryCallTarget extends RootNode {

    private ExecutableQuery executableQuery;

    public BFQueryCallTarget(TruffleLanguage<?> language, ExecutableQuery callTarget) {
        super(language, new FrameDescriptor());
        this.executableQuery = callTarget;
    }

    @Override
    public Object execute(VirtualFrame frame) {
        return executableQuery;
    }
}
