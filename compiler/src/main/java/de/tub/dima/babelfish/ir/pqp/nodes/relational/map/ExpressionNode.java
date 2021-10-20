package de.tub.dima.babelfish.ir.pqp.nodes.relational.map;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.ExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.LazyNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.NumericLibrary;

/**
 * Expression nodes for ADD, SUB, MUL, and DIV.
 * Each expression is defined for different data types.
 */
public abstract class ExpressionNode extends BFExecutableNode {

    protected ExpressionNode(TruffleLanguage<?> language) {

    }


    @NodeChild(value = "left", type = BFExecutableNode.class)
    @NodeChild(value = "right", type = BFExecutableNode.class)
    public static abstract class MulExpressionNode extends ExpressionNode {

        protected MulExpressionNode(TruffleLanguage<?> language) {
            super(language);
        }

        @Specialization
        public Int_8 mulInt8(Int_8 val1, Int_8 val2) {
            return new Eager_Int_8((byte) (val1.asByte() * val2.asByte()));
        }

        @Specialization
        public Int_16 mulInt16(Int_16 val1, Int_16 val2) {
            return new Eager_Int_16((short) (val1.asShort() * val2.asShort()));
        }

        @Specialization
        public Int_32 mulInt32(Int_32 val1, Int_32 val2) {
            return new Eager_Int_32((val1.asInt() * val2.asInt()));
        }

        @Specialization
        public Int_64 mulInt8(Int_64 val1, Int_64 val2) {
            return new Eager_Int_64((val1.asLong() * val2.asLong()));
        }

        @Specialization
        public Float_32 mulInt8(Float_32 val1, Float_32 val2) {
            return new Eager_Float_32(val1.asFloat() * val2.asFloat());
        }

        @Specialization
        public Float_64 mulInt8(Float_64 val1, Float_64 val2) {
            return new Eager_Float_64(val1.asDouble() * val2.asDouble());
        }

        @Specialization(guards = "left.getPrecision() == right.getPrecision()")
        public EagerNumeric mulNumeric(Numeric left,
                                       Numeric right,
                                       @Cached(value = "right.getPrecision()", allowUncached = true) int precision,
                                       @CachedLibrary(limit = "30") NumericLibrary leftLib,
                                       @CachedLibrary(limit = "30") NumericLibrary rightLib) {
            long leftValue = leftLib.getValue(left);
            long rightValue = rightLib.getValue(right);
            return new EagerNumeric(leftValue * rightValue, precision * 2);
        }

        @Specialization()
        public EagerNumeric mulNumeric(Numeric left,
                                       Numeric right,
                                       @Cached(value = "left.getPrecision()", allowUncached = true) int val1_precision,
                                       @Cached(value = "right.getPrecision()", allowUncached = true) int val2_precision,
                                       @CachedLibrary(limit = "30") NumericLibrary leftLib,
                                       @CachedLibrary(limit = "30") NumericLibrary rightLib) {
            long leftValue = leftLib.getValue(left);
            long rightValue = rightLib.getValue(right);
            return new EagerNumeric(leftValue * rightValue, val1_precision + val2_precision);
        }
    }

    @NodeChild(value = "left", type = BFExecutableNode.class)
    @NodeChild(value = "right", type = BFExecutableNode.class)
    public static abstract class AddExpressionNode extends ExpressionNode {

        protected AddExpressionNode(TruffleLanguage<?> language) {
            super(language);
        }

        @Specialization
        public Int_8 mulInt8(Int_8 val1, Int_8 val2) {
            return new Eager_Int_8((byte) (val1.asByte() + val2.asByte()));
        }

        @Specialization
        public Int_16 mulInt16(Int_16 val1, Int_16 val2) {
            return new Eager_Int_16((short) (val1.asShort() + val2.asShort()));
        }

        @Specialization
        public Int_32 mulInt32(Int_32 val1, Int_32 val2) {
            return new Eager_Int_32((val1.asInt() + val2.asInt()));
        }

        @Specialization
        public Int_64 mulInt8(Int_64 val1, Int_64 val2) {
            return new Eager_Int_64((val1.asLong() + val2.asLong()));
        }

        @Specialization
        public Float_32 mulInt8(Float_32 val1, Float_32 val2) {
            return new Eager_Float_32(val1.asFloat() + val2.asFloat());
        }


        @Specialization
        public Float_32 mulInt8(float val1, Float_32 val2) {
            return new Eager_Float_32(val1 + val2.asFloat());
        }

        @Specialization
        public Float_64 mulInt8(Float_64 val1, Float_64 val2) {
            return new Eager_Float_64(val1.asDouble() + val2.asDouble());
        }

        @Specialization(guards = "val1.getPrecision() == val2.getPrecision()")
        public Numeric addNumeric(LazyNumeric val1, LazyNumeric val2, @Cached(value = "val1.getPrecision()", allowUncached = true) int precision) {
            return new EagerNumeric(val1.getValue() + val2.getValue(), precision);
        }

        @Specialization(guards = "val1.getPrecision() == val2.getPrecision()")
        public Numeric addNumeric(LazyNumeric val1, EagerNumeric val2, @Cached(value = "val1.getPrecision()", allowUncached = true) int precision) {
            return new EagerNumeric(val1.getValue() + val2.getValue(), precision);
        }

        @Specialization(guards = "val1.getPrecision() == val2.getPrecision()")
        public Numeric addNumeric(EagerNumeric val1, LazyNumeric val2, @Cached(value = "val1.getPrecision()", allowUncached = true) int precision) {
            return new EagerNumeric(val1.getValue() + val2.getValue(), precision);
        }


        @Specialization(guards = "val1.getPrecision() == val2.getPrecision()")
        public Numeric addNumeric(EagerNumeric val1, EagerNumeric val2, @Cached(value = "val1.getPrecision()", allowUncached = true) int precision) {
            return new EagerNumeric(val1.getValue() + val2.getValue(), precision);
        }

    }

    @NodeChild(value = "left", type = BFExecutableNode.class)
    @NodeChild(value = "right", type = BFExecutableNode.class)
    public static abstract class SubExpressionNode extends ExpressionNode {

        protected SubExpressionNode(TruffleLanguage<?> language) {
            super(language);
        }

        @Specialization
        public Int_8 mulInt8(Int_8 val1, Int_8 val2) {
            return new Eager_Int_8((byte) (val1.asByte() - val2.asByte()));
        }

        @Specialization
        public Int_16 mulInt16(Int_16 val1, Int_16 val2) {
            return new Eager_Int_16((short) (val1.asShort() - val2.asShort()));
        }

        @Specialization
        public Int_32 mulInt32(Int_32 val1, Int_32 val2) {
            return new Eager_Int_32((val1.asInt() - val2.asInt()));
        }

        @Specialization
        public Int_64 mulInt8(Int_64 val1, Int_64 val2) {
            return new Eager_Int_64((val1.asLong() - val2.asLong()));
        }

        @Specialization
        public Float_32 mulInt8(float val1, Float_32 val2) {
            return new Eager_Float_32(val1 - val2.asFloat());
        }

        @Specialization
        public Float_32 mulInt8(Float_32 val1, Float_32 val2) {
            return new Eager_Float_32(val1.asFloat() - val2.asFloat());
        }

        @Specialization
        public Float_64 mulInt8(Float_64 val1, Float_64 val2) {
            return new Eager_Float_64(val1.asDouble() - val2.asDouble());
        }

        @Specialization(guards = "val1.getPrecision() == val2.getPrecision()")
        public EagerNumeric subNumeric(LazyNumeric val1, EagerNumeric val2, @Cached(value = "val1.getPrecision()", allowUncached = true) int precision) {
            return new EagerNumeric(val1.getValue() - val2.getValue(), precision);
        }

        @Specialization(guards = "val1.getPrecision() == val2.getPrecision()")
        public EagerNumeric subNumeric(LazyNumeric val1, LazyNumeric val2, @Cached(value = "val1.getPrecision()", allowUncached = true) int precision) {
            return new EagerNumeric(val1.getValue() - val2.getValue(), precision);
        }

        @Specialization(guards = "val1.getPrecision() == val2.getPrecision()")
        public EagerNumeric subNumeric(EagerNumeric val1, EagerNumeric val2, @Cached(value = "val1.getPrecision()", allowUncached = true) int precision) {
            return new EagerNumeric(val1.getValue() - val2.getValue(), precision);
        }

        @Specialization(guards = "val1.getPrecision() == val2.getPrecision()")
        public EagerNumeric subNumeric(Numeric val1, Numeric val2, @Cached(value = "val1.getPrecision()", allowUncached = true) int precision) {
            return new EagerNumeric(val1.getValue() - val2.getValue(), precision);
        }


    }

    @NodeChild(value = "left", type = BFExecutableNode.class)
    @NodeChild(value = "right", type = BFExecutableNode.class)
    public static abstract class DivExpressionNode extends ExpressionNode {

        protected DivExpressionNode(TruffleLanguage<?> language) {
            super(language);
        }

        @Specialization
        public Int_32 divInt(int val1, int val2) {
            return new Eager_Int_32(val1 / val2);
        }

        @Specialization
        public Int_64 divInt(long val1, long val2) {
            return new Eager_Int_64(val1 / val2);
        }

        @Specialization
        public Int_32 divInt(Eager_Int_32 val1, int val2) {
            return new Eager_Int_32(val1.asIntValue() / val2);
        }


        @Specialization
        public Int_8 mulInt8(Int_8 val1, Int_8 val2) {
            return new Eager_Int_8((byte) (val1.asByte() / val2.asByte()));
        }

        @Specialization
        public Int_16 mulInt16(Int_16 val1, Int_16 val2) {
            return new Eager_Int_16((short) (val1.asShort() / val2.asShort()));
        }

        @Specialization
        public Int_32 mulInt32(Int_32 val1, Int_32 val2) {
            return new Eager_Int_32((val1.asInt() / val2.asInt()));
        }

        @Specialization
        public Int_64 mulInt8(Int_64 val1, Int_64 val2) {
            return new Eager_Int_64((val1.asLong() / val2.asLong()));
        }

        @Specialization
        public Float_32 mulInt8(Float_32 val1, Float_32 val2) {
            return new Eager_Float_32(val1.asFloat() / val2.asFloat());
        }

        @Specialization
        public Float_64 mulInt8(Float_64 val1, Float_64 val2) {
            return new Eager_Float_64(val1.asDouble() / val2.asDouble());
        }

        @Specialization(guards = "val1.getPrecision() == val2.getPrecision()")
        public Numeric divNumeric(Numeric val1, Numeric val2, @Cached(value = "val1.getPrecision()", allowUncached = true) int precision) {
            return new EagerNumeric(val1.getValue() / val2.getValue(), precision);
        }

    }
}
