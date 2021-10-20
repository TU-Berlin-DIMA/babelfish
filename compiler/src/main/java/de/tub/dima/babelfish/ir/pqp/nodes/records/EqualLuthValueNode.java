package de.tub.dima.babelfish.ir.pqp.nodes.records;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.DirectCallNode;
import com.oracle.truffle.api.nodes.RootNode;
import de.tub.dima.babelfish.ir.pqp.nodes.types.TextEqualNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.TypedCallNode;
import de.tub.dima.babelfish.typesytem.udt.AbstractDate;
import de.tub.dima.babelfish.typesytem.udt.DateLibrary;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.IntLibrary;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_64;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

@NodeChild(type = ArgumentReadNode.class)
@NodeChild(type = ArgumentReadNode.class)
public abstract class EqualLuthValueNode extends RootNode {

    protected EqualLuthValueNode() {
        super(null);
    }

    public static TypedCallNode<Boolean> create() {
        return new TypedCallNode<Boolean>(EqualLuthValueNodeGen.create(new ArgumentReadNode(0), new ArgumentReadNode(1)));
    }

    @Specialization
    boolean equals(Int_32 val1, Int_32 val2, @CachedLibrary(limit = "30") IntLibrary lib) {
        return lib.asIntValue(val1) == lib.asIntValue(val2);
    }

    @Specialization
    boolean equals(Float_32 val1, float val2) {
        return val1.asFloat() == val2;
    }

    @Specialization
    boolean equals(Float_32 val1, Float_32 val2) {
        return val1.asFloat() == val2.asFloat();
    }


    @Specialization
    boolean equals(Float_64 val1, double val2) {
        return val1.asDouble() == val2;
    }

    @Specialization
    boolean equals(AbstractDate val1, AbstractDate val2, @CachedLibrary(limit = "30") DateLibrary lib) {
        return lib.getTs(val1) == lib.getTs(val2);
    }


    @Specialization
    boolean equals(Char val1, Char val2) {
        return val1.getChar() == val2.getChar();
    }


    public DirectCallNode adoptCallTarget() {
        CallTarget callTarget = Truffle.getRuntime().createCallTarget(TextEqualNode.create(null));
        return Truffle.getRuntime().createDirectCallNode(callTarget);
    }

    @Specialization()
    boolean equals(Text val1, Text val2, @Cached(value = "adoptCallTarget()", allowUncached = true) DirectCallNode call) {
        return (boolean) call.call(val1, val2);
    }

}
