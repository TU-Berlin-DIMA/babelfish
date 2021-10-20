package de.tub.dima.babelfish.ir.pqp.nodes.utils;

import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;

public class ArgumentReadNode extends BFExecutableNode {

    private final int index;

    public ArgumentReadNode(int index) {
        this.index = index;
    }

    @Override
    public Object execute(VirtualFrame frame) {
        return frame.getArguments()[index];
    }

    @Override
    public boolean isInstrumentable() {
        return false;
    }
}
