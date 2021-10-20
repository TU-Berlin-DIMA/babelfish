package de.tub.dima.babelfish.ir.pqp.nodes.records;

import com.oracle.truffle.api.dsl.NodeChild;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;

@NodeChild(value = "arguments", type = BFExecutableNode[].class)
public abstract class LuthBuiltinNodeLuth extends BFExecutableNode {

    private final BabelfishEngine.BabelfishContext context;

    protected LuthBuiltinNodeLuth(BabelfishEngine.BabelfishContext context) {
        this.context = context;
    }

    public final BabelfishEngine.BabelfishContext getContext() {
        return context;
    }

    @Override
    public boolean isInstrumentable() {
        return true;
    }

}
