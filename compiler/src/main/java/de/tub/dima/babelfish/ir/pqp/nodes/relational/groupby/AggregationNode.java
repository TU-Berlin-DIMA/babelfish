package de.tub.dima.babelfish.ir.pqp.nodes.relational.groupby;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.Node;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.pqp.nodes.records.ReadLuthFieldNode;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.storage.layout.fields.Numeric_PhysicalField;
import de.tub.dima.babelfish.storage.layout.fields.PhysicalFieldFactory;
import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.LazyNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.NumericLibrary;

public abstract class AggregationNode extends Node {

    protected final FieldReference reference;

    AggregationNode(FieldReference ref) {
        reference = ref;
    }

    public FieldReference getReference() {
        return reference;
    }

    public PhysicalField getPhysicalField() {
        if (Numeric.class.isAssignableFrom(reference.getType())) {
            return new Numeric_PhysicalField(reference.getName(), 2);

        } else {
            return PhysicalFieldFactory.getPhysicalField(reference.getType(), reference.getName());
        }
    }

    ;

    public abstract BFType execute(VirtualFrame virtualFrame);

    public abstract BFType getDefaultValue();

    @NodeChild(type = ReadLuthFieldNode.class)
    public static abstract class CountNode extends AggregationNode {


        CountNode(FieldReference ref) {
            super(ref);
        }

        public static CountNode create(String name, FrameSlot stateFrameSlot) {
            ReadLuthFieldNode readLuthFieldNode = new ReadLuthFieldNode(name, stateFrameSlot);
            return AggregationNodeFactory.CountNodeGen.create(new FieldReference<>(name, Int_32.class), readLuthFieldNode);
        }

        @Override
        public BFType getDefaultValue() {
            return new Eager_Int_32(0);
        }

        @Specialization
        public BFType increment(Eager_Int_32 var) {
            return new Eager_Int_32(var.asInt() + 1);
        }
    }

    @NodeChild(type = ReadLuthFieldNode.class, value = "object")
    @NodeChild(type = ReadLuthFieldNode.class, value = "var")
    public static abstract class MinNode extends AggregationNode {


        MinNode(FieldReference ref) {
            super(ref);
        }

        public static MinNode create(FrameDescriptor frameDescriptor, FieldReference stamp, String name, FrameSlot stateFrameSlot) {
            ReadLuthFieldNode recordField = new ReadLuthFieldNode(stamp.getKey(), frameDescriptor.findOrAddFrameSlot("object"));
            ReadLuthFieldNode stateField = new ReadLuthFieldNode(name, stateFrameSlot);
            return AggregationNodeFactory.MinNodeGen.create(new FieldReference<>(name, stamp.getType()), recordField, stateField);
        }

        @Override
        public BFType getDefaultValue() {
            return new EagerNumeric(Integer.MAX_VALUE, 2);
        }

        @Specialization
        public BFType min(Numeric value, EagerNumeric agg, @CachedLibrary(limit = "30") NumericLibrary lib) {
            if (lib.getValue(value) < lib.getValue(agg)) {
                return new EagerNumeric(lib.getValue(value), 2);
            } else {
                return new EagerNumeric(lib.getValue(agg), 2);
            }
        }
    }

    @NodeChild(type = ReadLuthFieldNode.class, value = "object")
    @NodeChild(type = ReadLuthFieldNode.class, value = "var")
    public static abstract class SumNode extends AggregationNode {


        SumNode(FieldReference ref) {
            super(ref);
        }

        public static SumNode create(FrameDescriptor frameDescriptor, FieldReference stamp, String name, FrameSlot stateFrameSlot) {
            ReadLuthFieldNode recordField = new ReadLuthFieldNode(stamp.getKey(), frameDescriptor.findOrAddFrameSlot("object"));
            ReadLuthFieldNode stateField = new ReadLuthFieldNode(name, stateFrameSlot);
            return AggregationNodeFactory.SumNodeGen.create(new FieldReference<>(name, stamp.getType()), recordField, stateField);
        }

        @Override
        public BFType getDefaultValue() {
            if (Float_32.class.isAssignableFrom(reference.getType())) {
                return new Eager_Float_32(0);
            } else if (Float_64.class.isAssignableFrom(reference.getType())) {
                return new Eager_Float_64(0);
            } else if (Int_8.class.isAssignableFrom(reference.getType())) {
                return new Eager_Int_8((byte) 0);
            } else if (Int_16.class.isAssignableFrom(reference.getType())) {
                return new Eager_Int_16(((short) 0));
            } else if (Int_32.class.isAssignableFrom(reference.getType())) {
                return new Eager_Int_32(0);
            } else if (Int_64.class.isAssignableFrom(reference.getType())) {
                return new Eager_Int_64(0);
            } else if (Numeric.class.isAssignableFrom(reference.getType())) {
                return new EagerNumeric(0, 4);
            }
            return null;
        }

        @Specialization()
        public BFType addFloat(Eager_Float_32 value, Eager_Float_32 aggregate) {
            return new Eager_Float_32(value.asFloat() + aggregate.asFloat());
        }

        @Specialization()
        public BFType addFloat(LazyFloat_32 value, Eager_Float_32 aggregate) {
            return new Eager_Float_32(value.asFloat() + aggregate.asFloat());
        }


        @Specialization()
        public BFType add(LazyNumeric value, EagerNumeric aggregate) {
            return new EagerNumeric(value.getValue() + aggregate.value, value.getPrecision());
        }

        @Specialization()
        public BFType add(EagerNumeric value, EagerNumeric aggregate) {
            return new EagerNumeric(value.getValue() + aggregate.value, value.getPrecision());
        }

        @Specialization()
        public BFType add(Lazy_Int_32 value, Eager_Int_32 aggregate) {
            return new Eager_Int_32(value.asInt() + aggregate.asInt());
        }

        @Specialization()
        public BFType add(Eager_Int_32 value, Eager_Int_32 aggregate) {
            return new Eager_Int_32(value.asInt() + aggregate.asInt());
        }

    }

}
