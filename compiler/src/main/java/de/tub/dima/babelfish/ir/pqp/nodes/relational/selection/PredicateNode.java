package de.tub.dima.babelfish.ir.pqp.nodes.relational.selection;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.*;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.DirectCallNode;
import com.oracle.truffle.api.nodes.ExecutableNode;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.records.ReadLuthFieldNode;
import de.tub.dima.babelfish.ir.pqp.nodes.types.TextEqualNode;
import de.tub.dima.babelfish.typesytem.udt.AbstractDate;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.udt.DateLibrary;
import de.tub.dima.babelfish.typesytem.valueTypes.Bool;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Lazy_Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.NumericLibrary;
import de.tub.dima.babelfish.typesytem.variableLengthType.EagerText;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

/**
 * Defines different logical predicate nodes to compare BF data types.
 */
public abstract class PredicateNode extends BFExecutableNode {


    public static abstract class BinaryPredicateNode extends PredicateNode {
        @Child
        private PredicateNode left;
        @Child
        private PredicateNode right;

        protected BinaryPredicateNode(PredicateNode left, PredicateNode right) {
            this.left = left;
            this.right = right;
        }

        public PredicateNode getLeft() {
            return left;
        }

        public PredicateNode getRight() {
            return right;
        }
    }

    public static class AndNode extends BinaryPredicateNode {

        public AndNode(PredicateNode left, PredicateNode right) {
            super(left, right);
        }

        @Override
        public Object execute(VirtualFrame object) {
            return getLeft().executeAsBoolean(object) & getRight().executeAsBoolean(object);
        }
    }

    public static class OrNode extends BinaryPredicateNode {

        public OrNode(PredicateNode left, PredicateNode right) {
            super(left, right);
        }

        @Override
        public Object execute(VirtualFrame object) {
            return getLeft().executeAsBoolean(object) || getRight().executeAsBoolean(object);
        }
    }

    @NodeChild(value = "left", type = PredicateItemNode.class)
    @NodeChild(value = "right", type = PredicateItemNode.class)
    @NodeInfo(shortName = "GreaterThenNode")
    public abstract static class GreaterThanNode extends PredicateNode {

        public static PredicateNode create(PredicateNode.PredicateItemNode left, PredicateNode.PredicateItemNode right) {
            return PredicateNodeFactory.GreaterThanNodeGen.create(left, right);
        }

        @Specialization
        boolean grater(Float_32 val1, float val2) {
            return val1.asFloat() > val2;
        }

        @Specialization
        boolean grater(Float_32 val1, Float_32 val2) {
            return val1.asFloat() > val2.asFloat();
        }

        @Specialization
        boolean grater(Float_64 val1, Float_64 val2) {
            return val1.asDouble() > val2.asDouble();
        }

        @Specialization
        boolean grater(Float_64 val1, double val2) {
            return val1.asDouble() > val2;
        }

        @Specialization
        boolean grater(Int_64 val1, long val2) {
            return val1.asLong() > val2;
        }

        @Specialization
        boolean grater(Int_32 val1, int val2) {
            return val1.asInt() > val2;
        }

        @Specialization
        boolean grater(Date val1, Date val2) {
            return val1.compareTo(val2) > 0;
        }

        @Specialization
        boolean grater(Date val1, long val2) {
            return val1.unixTs > val2;
        }

        @Specialization
        boolean grater(AbstractDate val1, long val2) {
            return val1.getUnixTs() > val2;
        }

        @Specialization
        boolean grater(Numeric val1, Numeric val2) {
            return val1.getValue() > val2.getValue();
        }

        @Override
        public Object execute(VirtualFrame object) {
            throw new ArithmeticException();
        }
    }

    @NodeChild(value = "left", type = BFExecutableNode.class)
    @NodeChild(value = "right", type = BFExecutableNode.class)
    @NodeInfo(shortName = "GreaterEqualsNode")
    public abstract static class GreaterEqualsNode extends PredicateNode {

        public static PredicateNode create(BFExecutableNode left, BFExecutableNode right) {
            return PredicateNodeFactory.GreaterEqualsNodeGen.create(left, right);
        }

        @Specialization
        boolean graterEquals(Numeric val1, Numeric val2, @CachedLibrary(limit = "30") NumericLibrary lib) {
            return lib.getValue(val1) >= lib.getValue(val2);
        }

        @Specialization
        boolean graterEquals(Float_32 val1, float val2) {
            return val1.asFloat() >= val2;
        }

        @Specialization
        boolean graterEquals(Float_32 val1, Float_32 val2) {
            return val1.asFloat() >= val2.asFloat();
        }

        @Specialization
        boolean graterEquals(Float_64 val1, Float_64 val2) {
            return val1.asDouble() >= val2.asDouble();
        }

        @Specialization
        boolean graterEquals(Float_64 val1, double val2) {
            return val1.asDouble() >= val2;
        }

        @Specialization
        boolean graterEquals(Int_32 val1, Int_32 val2) {
            return val1.asInt() > val2.asInt();
        }

        @Specialization
        boolean graterEquals(Int_32 val1, int val2) {
            return val1.asInt() >= val2;
        }

        @Specialization
        boolean graterEquals(Date val1, Date val2) {
            return val1.compareTo(val2) >= 0;
        }


        @Specialization
        boolean graterEquals(AbstractDate val1, long val2, @CachedLibrary(limit = "30") DateLibrary lib) {
            return lib.getTs(val1) >= val2;
        }

        @Override
        public Object execute(VirtualFrame object) {
            throw new ArithmeticException();
        }
    }


    @NodeChild(value = "left", type = BFExecutableNode.class)
    @NodeChild(value = "right", type = BFExecutableNode.class)
    @NodeInfo(shortName = "LessThanNode")
    public abstract static class LessThanNode extends PredicateNode {

        public static PredicateNode create(BFExecutableNode left, BFExecutableNode right) {
            return PredicateNodeFactory.LessThanNodeGen.create(left, right);
        }

        @Specialization
        boolean less(Float_32 val1, Float_32 val2) {
            return val1.asFloat() < val2.asFloat();
        }

        @Specialization
        boolean less(Float_32 val1, float val2) {
            return val1.asFloat() < val2;
        }

        @Specialization
        boolean less(Float_64 val1, Float_64 val2) {
            return val1.asDouble() < val2.asDouble();
        }

        @Specialization
        boolean less(Float_64 val1, double val2) {
            return val1.asDouble() < val2;
        }

        @Specialization
        boolean less(Int_32 val1, int val2) {
            return val1.asInt() < val2;
        }

        @Specialization
        boolean less(Int_32 val1, Int_32 val2) {
            return val1.asInt() < val2.asInt();
        }

        @Specialization
        boolean less(Date val1, Date val2) {
            return val1.unixTs < val2.unixTs;
        }

        @Specialization
        boolean less(AbstractDate val1, long val2, @CachedLibrary(limit = "30") DateLibrary lib) {
            return lib.getTs(val1) < val2;
        }

        @Specialization
        boolean less(Numeric val1, Numeric val2, @CachedLibrary(limit = "30") NumericLibrary lib) {
            return lib.getValue(val1) < lib.getValue(val2);
        }

        @Override
        public Object execute(VirtualFrame object) {
            throw new ArithmeticException();
        }
    }

    @NodeChild(value = "left", type = PredicateItemNode.class)
    @NodeChild(value = "right", type = PredicateItemNode.class)
    @NodeInfo(shortName = "LessEqualsNode")
    public abstract static class LessEqualsNode extends PredicateNode {

        public static PredicateNode create(PredicateNode.PredicateItemNode left, PredicateNode.PredicateItemNode right) {
            return PredicateNodeFactory.LessEqualsNodeGen.create(left, right);
        }

        @Specialization
        boolean lessEquals(Float_32 val1, Float_32 val2) {
            return val1.asFloat() <= val2.asFloat();
        }

        @Specialization
        boolean lessEquals(Float_32 val1, float val2) {
            return val1.asFloat() <= val2;
        }

        @Specialization
        boolean lessEquals(Float_64 val1, Float_64 val2) {
            return val1.asDouble() <= val2.asDouble();
        }

        @Specialization
        boolean lessEquals(Float_64 val1, double val2) {
            return val1.asDouble() <= val2;
        }

        @Specialization
        boolean lessEquals(Int_32 val1, int val2) {
            return val1.asInt() <= val2;
        }

        @Specialization
        boolean lessEquals(Int_32 val1, Int_32 val2) {
            return val1.asInt() <= val2.asInt();
        }

        @Specialization
        boolean lessEquals(Date val1, Date val2) {
            return val1.unixTs <= val2.unixTs;
        }

        @Specialization
        boolean lessEquals(Date val1, long val2) {
            return val1.unixTs <= val2;
        }

        @Specialization
        boolean lessEquals(AbstractDate val1, long val2) {
            return val1.getUnixTs() <= val2;
        }

        @Specialization
        boolean lessEquals(Numeric val1, Numeric val2, @CachedLibrary(limit = "30") NumericLibrary lib) {
            return lib.getValue(val1) <= lib.getValue(val2);
        }

        @Override
        public Object execute(VirtualFrame object) {
            throw new ArithmeticException();
        }
    }

    @NodeChild(value = "left", type = BFExecutableNode.class)
    @NodeChild(value = "right", type = BFExecutableNode.class)
    @NodeInfo(shortName = "EqualNode")
    public abstract static class EqualNode extends PredicateNode {


        public static PredicateNode create(BFExecutableNode left, BFExecutableNode right) {
            return PredicateNodeFactory.EqualNodeGen.create(left, right);
        }


        @Specialization
        boolean equalsInt(Lazy_Int_32 val1, int val2) {
            return val1.asInt() == val2;
        }

        @Specialization
        boolean equalsInt(Eager_Int_32 val1, int val2) {
            return val1.asInt() == val2;
        }


        public DirectCallNode adoptCallTarget() {
            CallTarget callTarget = Truffle.getRuntime().createCallTarget(TextEqualNode.create(null));
            return Truffle.getRuntime().createDirectCallNode(callTarget);
        }

        @Specialization()
        boolean equalsText(Text val1, Text val2, @Cached(value = "adoptCallTarget()", allowUncached = true) DirectCallNode call) {
            return (boolean) call.call(val1, val2);
        }

        @Specialization()
        boolean equalsText(Text val1, String val2, @Cached(value = "adoptCallTarget()", allowUncached = true) DirectCallNode call) {
            StringText v2 = new StringText(val2);
            return (boolean) call.call(val1, v2);
        }

        @Specialization()
        boolean equalsBool(Bool val1, boolean val2) {
            return val1.getValue() == val2;
        }

        @Fallback
        boolean equalsFallback(Object val1, Object val2) {
            return val1.equals(val2);
        }
    }

    public static abstract class PredicateItemNode extends BFExecutableNode {

        protected PredicateItemNode(TruffleLanguage<?> language) {

        }

        public abstract Object execute(VirtualFrame object);

    }

    @NodeInfo(shortName = "ReadFieldNode")
    public static class ReadFieldNode extends PredicateItemNode {

        @Child
        ReadLuthFieldNode readNode;

        public ReadFieldNode(String fieldName, FrameDescriptor frameDescriptor, TruffleLanguage<?> language) {
            super(language);
            FrameSlot inputObjectSlot = frameDescriptor.findOrAddFrameSlot("object", FrameSlotKind.Object);
            readNode = new ReadLuthFieldNode(fieldName, inputObjectSlot);
        }

        public Object execute(VirtualFrame object) {
            return readNode.execute(object);
        }

    }


    @NodeInfo(description = "ConstantValue($value)")
    @NodeField(name = "value", type = Object.class)
    public abstract static class ConstantValueNode extends PredicateItemNode {

        protected ConstantValueNode(TruffleLanguage<?> language) {
            super(language);
        }

        public boolean isDate(Object value) {
            return value instanceof Date;
        }

        public boolean isFloat32(Object value) {
            return value instanceof Float_32;
        }

        public boolean isFloat64(Object value) {
            return value instanceof Float_64;
        }

        public boolean isInt32(Object value) {
            return value instanceof Int_32;
        }

        public long asDate(Object value) {
            return ((Date) value).unixTs;
        }

        public float asFloat(Object value) {
            return ((Float_32) value).asFloat();
        }

        public double asDouble(Object value) {
            return ((Float_64) value).asDouble();
        }

        public int asInt32(Object value) {
            return ((Int_32) value).asInt();
        }

        public boolean isString(Object value) {
            return value instanceof String;
        }

        public String asString(Object value) {
            return (String) value;
        }

        @Specialization(guards = "isDate(value)")
        public long getValue(@Cached(value = "asDate(value)", allowUncached = true) long cached_value) {
            return cached_value;
        }

        @Specialization(guards = "isFloat32(value)")
        public float getValue(@Cached(value = "asFloat(value)", allowUncached = true) float cached_value) {
            return cached_value;
        }

        @Specialization(guards = "isFloat64(value)")
        public double getValue(@Cached(value = "asDouble(value)", allowUncached = true) double cached_value) {
            return cached_value;
        }

        @Specialization(guards = "isInt32(value)")
        public int getValue(@Cached(value = "asInt32(value)", allowUncached = true) int cached_value) {
            return cached_value;
        }

        @Specialization(guards = "isString(value)")
        public StringText getValue(@Cached(value = "asString(value)", allowUncached = true) String cached_value) {
            return new StringText(cached_value);
        }


        @Specialization
        public Object getValueFallback(Object value) {
            return value;
        }
    }


}
