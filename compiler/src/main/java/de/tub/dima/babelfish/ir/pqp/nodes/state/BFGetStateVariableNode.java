package de.tub.dima.babelfish.ir.pqp.nodes.state;


import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.Node;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateManager;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateVariable;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;

public class BFGetStateVariableNode extends Node {

    @CompilerDirectives.CompilationFinal
    private final StateDescriptor stateDescriptor;
    private final int stateIndex;

    public BFGetStateVariableNode(StateDescriptor stateDescriptor, int stateIndex) {
        this.stateDescriptor = stateDescriptor;
        this.stateIndex = stateIndex;
    }

    @ExplodeLoop
    public BFStateVariable getStateVariable(BFStateManager stateManager) {
        return stateManager.getStateVariable(stateIndex);
    }


}
