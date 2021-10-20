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
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.storage.layout.fields.Fload_32_PhysicalField;
import de.tub.dima.babelfish.storage.layout.fields.Numeric_PhysicalField;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;

@NodeChild(type = ArgumentReadNode.class, value = "stateVariable")
@NodeField(type = StateDescriptor.class, name = "stateDescriptor")
@NodeField(type = boolean.class, name = "multithreaded")
public abstract class InitStateVariableNode extends ExecutableNode {
    protected InitStateVariableNode() {
        super(null);
    }

    public static InitStateVariableNode create(StateDescriptor stateDescriptor) {
        return InitStateVariableNodeGen.create(new ArgumentReadNode(0), stateDescriptor, RuntimeConfiguration.MULTI_THREADED);
    }

    public PhysicalField getPhysicalField(StateDescriptor stateDescriptor) {
        return stateDescriptor.getPhysicalSchema().getField(0);
    }

    public boolean isFloat32(StateDescriptor stateDescriptor) {
        return getPhysicalField(stateDescriptor) instanceof Fload_32_PhysicalField;
    }

    public boolean isNumeric(StateDescriptor stateDescriptor) {
        return getPhysicalField(stateDescriptor) instanceof Numeric_PhysicalField;
    }

    public Numeric_PhysicalField getNumericField(StateDescriptor stateDescriptor) {
        return (Numeric_PhysicalField) getPhysicalField(stateDescriptor);
    }

    public Fload_32_PhysicalField getFloatField(StateDescriptor stateDescriptor) {
        return (Fload_32_PhysicalField) getPhysicalField(stateDescriptor);
    }

    public Numeric getNumericDefault(StateDescriptor stateDescriptor) {
        return (Numeric) stateDescriptor.getDefaultValue()[0];
    }

    public Float_32 getFloatDefault(StateDescriptor stateDescriptor) {
        return (Float_32) stateDescriptor.getDefaultValue()[0];
    }


    @Specialization(guards = "isNumeric(stateDescriptor)")
    public Object initNumeric(boolean multithreaded, StateVariable stateVariable,
                              @Cached(value = "getNumericField(stateDescriptor)", allowUncached = true) Numeric_PhysicalField field,
                              @Cached(value = "getNumericDefault(stateDescriptor)", allowUncached = true) Numeric defaultValue
    ) {
        long address = stateVariable.getAddress();

        if (multithreaded) {
            UnsafeUtils.putInt(address, 0);
            // field.writeValue(new AddressPointer(address + BFParallelGroupByOperator.LOCK_OFFSET), defaultValue);
        } else {
            field.writeValue(new AddressPointer(address), defaultValue);
        }

        return null;
    }

    @Specialization(guards = "isFloat32(stateDescriptor)")
    public Object initFloat32(StateVariable stateVariable,
                              @Cached(value = "getFloatField(stateDescriptor)", allowUncached = true) Fload_32_PhysicalField field,
                              @Cached(value = "getFloatDefault(stateDescriptor)", allowUncached = true) Float_32 defaultValue) {
        long address = stateVariable.getAddress();
        field.writeValue(new AddressPointer(address), defaultValue);
        return null;
    }
}
