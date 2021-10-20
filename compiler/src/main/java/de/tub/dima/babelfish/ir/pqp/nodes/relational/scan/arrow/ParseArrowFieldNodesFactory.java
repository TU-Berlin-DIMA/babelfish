package de.tub.dima.babelfish.ir.pqp.nodes.relational.scan.arrow;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeField;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.storage.text.leaf.ArrowSourceRope;
import de.tub.dima.babelfish.storage.text.leaf.ByteArrayConstantRope;
import de.tub.dima.babelfish.typesytem.udt.ArrowSourceDate;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.ArrowSourceInt_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.ArrowSourceNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.*;

public class ParseArrowFieldNodesFactory {

    public static ParseFieldNode createNode(ValueVector vector) {
        if (vector instanceof IntVector) {
            return IntFieldNode.create((IntVector) vector);
        } else if (vector instanceof DecimalVector) {
            return NumericFieldNode.create((DecimalVector) vector);
        } else if (vector instanceof TimeMilliVector) {
            return DateFieldNode.create((TimeMilliVector) vector);
        } else if (vector instanceof VarCharVector) {
            return TextFieldNode.create((VarCharVector) vector);
        } else {
            return UndefinedField.create();
        }
    }

    @NodeInfo
    @NodeChild(type = ArgumentReadNode.class, value = "index")
    public static abstract class ParseFieldNode extends Node {

        protected final boolean LAZY_PARSING = RuntimeConfiguration.LAZY_PARSING;

        public abstract Object execute(VirtualFrame frame);

    }

    @NodeInfo
    @NodeField(name = "vector", type = IntVector.class)
    public static abstract class IntFieldNode extends ParseFieldNode {

        public static IntFieldNode create(IntVector vector) {
            return ParseArrowFieldNodesFactoryFactory.IntFieldNodeGen.create(new ArgumentReadNode(0), vector);
        }

        public int getTypeWith() {
            return getVector().getTypeWidth();
        }

        @Specialization(guards = "LAZY_PARSING")
        ArrowSourceInt_32 parse(int index, @Cached("getTypeWith()") int typeWidth) {
            return new ArrowSourceInt_32(getVector().getDataBuffer(), (long) index * typeWidth);
        }


        @Specialization()
        Eager_Int_32 parse(int index) {
            return new Eager_Int_32(getVector().get(index));
        }

        abstract IntVector getVector();

    }

    @NodeInfo
    @NodeField(name = "vector", type = VarCharVector.class)
    public static abstract class TextFieldNode extends ParseFieldNode {

        public static TextFieldNode create(VarCharVector vector) {
            return ParseArrowFieldNodesFactoryFactory.TextFieldNodeGen.create(new ArgumentReadNode(0), vector);
        }

        @Specialization(guards = "LAZY_PARSING")
        ArrowSourceRope parseLazy(int index) {
            ArrowBufPointer charPointer = getVector().getDataPointer(index);
            return new ArrowSourceRope(charPointer, (int) charPointer.getLength());
        }

        @Specialization()
        ByteArrayConstantRope parse(int index) {
            byte[] value = getVector().get(index);
            return new ByteArrayConstantRope(value);
        }

        abstract VarCharVector getVector();

    }

    @NodeInfo
    @NodeField(name = "vector", type = TimeMilliVector.class)
    public static abstract class DateFieldNode extends ParseFieldNode {

        public static DateFieldNode create(TimeMilliVector vector) {
            return ParseArrowFieldNodesFactoryFactory.DateFieldNodeGen.create(new ArgumentReadNode(0), vector);
        }

        public int getTypeWith() {
            return getVector().getTypeWidth();
        }

        @Specialization(guards = "LAZY_PARSING")
        ArrowSourceDate parse(int index, @Cached("getTypeWith()") int typeWidth) {
            return new ArrowSourceDate(getVector().getDataBuffer(), index * typeWidth);
        }

        @Specialization()
        Date parse(int index) {
            int value = getVector().get(index);
            return new Date(value);
        }

        abstract TimeMilliVector getVector();

    }

    @NodeInfo
    @NodeField(name = "vector", type = DecimalVector.class)
    public static abstract class NumericFieldNode extends ParseFieldNode {

        public static NumericFieldNode create(DecimalVector vector) {
            return ParseArrowFieldNodesFactoryFactory.NumericFieldNodeGen.create(new ArgumentReadNode(0), vector);
        }

        public int getTypeWith() {
            return getVector().getTypeWidth();
        }

        public int getPrecision() {
            return getVector().getScale();
        }

        @Specialization(guards = "LAZY_PARSING")
        ArrowSourceNumeric parseLazy(int index,
                                     @Cached("getTypeWith()") int typeWidth,
                                     @Cached("getPrecision()") int precision) {
            return new ArrowSourceNumeric(getVector().getDataBuffer(),
                    (long) index * typeWidth,
                    precision);
        }

        @CompilerDirectives.TruffleBoundary(allowInlining = true)
        long parse(ArrowBuf dataBuffer, long offset) {
            return dataBuffer.getInt(offset);
        }

        @Specialization()
        EagerNumeric parse(int index) {
            //int value = getVector().getObject(index).unscaledValue().intValue();

            //NullableDecimalHolder holder = new NullableDecimalHolder();
            //getVector().get(index, holder);

            int value = getVector().getDataBuffer().getInt((long) index * getTypeWith());
            return new EagerNumeric(value, getVector().getScale());
        }

        abstract DecimalVector getVector();

    }

    @NodeInfo
    public static abstract class UndefinedField extends ParseFieldNode {

        public static UndefinedField create() {
            return ParseArrowFieldNodesFactoryFactory
                    .UndefinedFieldNodeGen.create(new ArgumentReadNode(0));
        }

        @Specialization()
        EagerNumeric parse(int index) {
            return null;
        }

    }

}
