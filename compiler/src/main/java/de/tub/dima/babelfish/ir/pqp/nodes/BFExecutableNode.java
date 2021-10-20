package de.tub.dima.babelfish.ir.pqp.nodes;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.instrumentation.*;

@GenerateWrapper
public abstract class BFExecutableNode extends BFBaseNode implements InstrumentableNode {

    /**
     * Executes this node using the specified context and frame and returns the result value.
     *
     * @param frame the frame of the currently executing guest language method
     * @return the value of the execution
     */
    public abstract Object execute(VirtualFrame frame);

    public boolean executeAsBoolean(VirtualFrame frame) {
        return (boolean) execute(frame);
    }

    public void executeAsVoid(VirtualFrame frame) {
        execute(frame);
    }

    @Override
    public boolean hasTag(Class<? extends Tag> tag) {
        if (tag == StandardTags.RootTag.class) {
            return true;
        }
        return false;
    }

    @Override
    public WrapperNode createWrapper(ProbeNode probe) {
        return null;
    }

    @Override
    public boolean isInstrumentable() {
        return false;
    }
}
