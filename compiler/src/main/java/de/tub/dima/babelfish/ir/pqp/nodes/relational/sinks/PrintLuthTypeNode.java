package de.tub.dima.babelfish.ir.pqp.nodes.relational.sinks;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Fallback;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.Node;
import de.tub.dima.babelfish.ir.pqp.nodes.records.ReadLuthFieldNode;
import de.tub.dima.babelfish.storage.text.operations.ReserveRope;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_16;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_8;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

@NodeChild(type = ReadLuthFieldNode.class, value = "value")
public abstract class PrintLuthTypeNode extends Node {

    public abstract void execute(VirtualFrame frame);

    @CompilerDirectives.TruffleBoundary
    public void println(Object value) {
        System.out.println(value);
    }

    @CompilerDirectives.TruffleBoundary
    public void print(Object value) {
        System.out.print(value);
    }

    @Specialization
    public void print(Float_32 value) {
        print(value.asFloat());
    }

    @Specialization
    public void print(Float_64 value) {
        print(value.asDouble() + ", ");
    }

    @Specialization
    public void print(Int_8 value) {
        print(value.asByte() + ", ");
    }

    @Specialization
    public void print(Int_16 value) {
        print(value.asShort() + ", ");
    }

    @Specialization
    public void print(Int_32 value) {
        print(value.asInt() + ", ");
    }

    @Specialization
    public void print(Int_64 value) {
        print(value.asLong() + ", ");
    }

    @Specialization
    public void print(Numeric value) {
        print(value.toString());
    }

    @Specialization
    public void print(Char value) {
        print(value.getChar());
    }

    @Specialization
    @ExplodeLoop
    public void print(ReserveRope value, @Cached(value = "value.length()", allowUncached = true) long length) {
        for (int i = 0; i < length; i++) {
            print(value.get(i));
        }
        println(",");
    }

    @Specialization()
    public void print(Text value) {
        for (int i = 0; i < value.length(); i++) {
            print(value.get(i));
        }
        println(",");
    }

    @Fallback
    public void printFallback(Object value) {
        print(value);
        println(",");
    }

}
