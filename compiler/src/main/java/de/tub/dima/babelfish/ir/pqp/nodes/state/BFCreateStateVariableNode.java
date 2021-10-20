package de.tub.dima.babelfish.ir.pqp.nodes.state;


import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.Node;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;

/**
 * Primitive operator node to create a state variable.
 */
public class BFCreateStateVariableNode extends Node {

    @CompilerDirectives.CompilationFinal
    private final StateDescriptor stateDescriptor;

    public BFCreateStateVariableNode(StateDescriptor stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    @ExplodeLoop
    public BFRecord initStateVariable() {
        BFRecord object = BFRecord.createObject(stateDescriptor.getSchema());
        for (int i = 0; i < stateDescriptor.getPhysicalSchema().getSize(); i++) {
            object.setValue(i, stateDescriptor.getDefaultValue()[i]);
        }
        return object;
    }


}
