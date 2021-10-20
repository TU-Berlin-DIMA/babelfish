package de.tub.dima.babelfish.ir.pqp.nodes.state;


import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeField;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.ExecutableNode;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateDescriptor;
import de.tub.dima.babelfish.ir.pqp.objects.state.StateVariable;
import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.storage.layout.fields.Int_32_PhysicalField;
import de.tub.dima.babelfish.storage.layout.fields.Numeric_PhysicalField;

@NodeChild(type = ArgumentReadNode.class, value = "stateVariable")
@NodeField(type = StateDescriptor.class, name = "stateDescriptor")
@NodeField(type = boolean.class, name = "multithreaded")
public abstract class ReadStateVariableNode extends ExecutableNode {

    protected ReadStateVariableNode() {
        super(null);
    }

    public static ReadStateVariableNode create(StateDescriptor stateDescriptor) {
        return ReadStateVariableNodeGen.create(new ArgumentReadNode(0), stateDescriptor, RuntimeConfiguration.MULTI_THREADED);
    }


    public static PhysicalField getPhysicalField(StateDescriptor stateDescriptor) {
        return stateDescriptor.getPhysicalSchema().getField(0);
    }

    public static boolean isNumeric(StateDescriptor stateDescriptor) {
        return getPhysicalField(stateDescriptor) instanceof Numeric_PhysicalField;
    }

    public static Numeric_PhysicalField getNumericField(StateDescriptor stateDescriptor) {
        return (Numeric_PhysicalField) getPhysicalField(stateDescriptor);
    }

    @Specialization(guards = "isNumeric(stateDescriptor)")
    public static Object readNumericField(boolean multithreaded, StateVariable var,
                                          @Cached(value = "getNumericField(stateDescriptor)", allowUncached = true) Numeric_PhysicalField physicalField) {

        if (multithreaded) {
            // return physicalField.readValue(new AddressPointer(var.address+ SyncLuthGroupBy.LOCK_OFFSET));
        } else {
            //return physicalField.readValue(new AddressPointer(var.address));
        }
        return null;


    }

    @Specialization
    public static Object read(StateVariable var) {
        return ((Int_32_PhysicalField) var.getPhysicalField()).readValue(new AddressPointer(0)).asInt();
    }


}
