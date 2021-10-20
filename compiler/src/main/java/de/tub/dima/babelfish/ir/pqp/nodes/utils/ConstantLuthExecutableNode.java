package de.tub.dima.babelfish.ir.pqp.nodes.utils;

import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;

public class ConstantLuthExecutableNode extends BFExecutableNode {

    private final Object value;

    public ConstantLuthExecutableNode(Object value) {
        this.value = value;
    }

    @Override
    public Object execute(VirtualFrame frame) {
        return value;
    }

    @Override
    public boolean isInstrumentable() {
        return false;
    }
}
