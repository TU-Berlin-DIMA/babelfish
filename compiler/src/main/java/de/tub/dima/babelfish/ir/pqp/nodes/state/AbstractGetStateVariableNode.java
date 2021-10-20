package de.tub.dima.babelfish.ir.pqp.nodes.state;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateVariable;

public abstract class AbstractGetStateVariableNode extends Node {

    @CompilerDirectives.CompilationFinal
    protected final String variableName;

    @CompilerDirectives.CompilationFinal
    protected int variableIndex = -1;

    protected AbstractGetStateVariableNode(String variableName) {
        this.variableName = variableName;
    }

    public abstract StateVariable execute(VirtualFrame frame);
}
