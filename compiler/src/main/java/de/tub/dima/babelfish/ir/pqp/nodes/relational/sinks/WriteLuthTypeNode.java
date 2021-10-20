package de.tub.dima.babelfish.ir.pqp.nodes.relational.sinks;

import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.Node;
import de.tub.dima.babelfish.buildins.AbstractRopeBuiltins;
import de.tub.dima.babelfish.ir.pqp.nodes.BFExecutableNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.storage.text.Rope;
import de.tub.dima.babelfish.storage.text.leaf.PointerRope;
import de.tub.dima.babelfish.storage.text.operations.ReserveRope;
import de.tub.dima.babelfish.storage.text.operations.SubstringRope;
import de.tub.dima.babelfish.typesytem.udt.AbstractDate;
import de.tub.dima.babelfish.typesytem.udt.DateLibrary;
import de.tub.dima.babelfish.typesytem.valueTypes.Bool;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.IntLibrary;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.NumericLibrary;
import de.tub.dima.babelfish.typesytem.variableLengthType.EagerText;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;

@NodeChild(type = BFExecutableNode.class, value = "value")
@NodeChild(type = ArgumentReadNode.class, value = "address")
public abstract class WriteLuthTypeNode extends Node {

    public static WriteLuthTypeNode createNode(BFExecutableNode value, ArgumentReadNode address) {
        return WriteLuthTypeNodeGen.create(value, address);
    }

    public abstract void execute(VirtualFrame frame);

    public char getChar(PointerRope value, int index) {
        return value.get(index);
    }


    public PointerRope getChild(ReserveRope rope) {
        return (PointerRope) rope.child;
    }

    public char getChar(ReserveRope value, int index) {
        return getChar(getChild(value), index);
    }

    public AbstractRopeBuiltins.GetCharTextNode getNode() {
        return AbstractRopeBuiltins.GetCharTextNode.create();
    }


    @Specialization
    public void write(SubstringRope value, long address,
                      @Cached(value = "getNode()") AbstractRopeBuiltins.GetCharTextNode getNode) {
        for (int i = 0; i < value.length(); i++) {
            // if (i < value.length()) {
            char character = (char) getNode.call(value, i);
            UnsafeUtils.putChar(address + (i * 2), character);
            //} else {
            //  UnsafeUtils.putChar(address + (i * 2), '\0');
            //}
        }
    }


    @Specialization
    @ExplodeLoop
    public void write(Rope value, long address,
                      @Cached(value = "value.length()", allowUncached = true) long length,
                      @Cached(value = "getNode()") AbstractRopeBuiltins.GetCharTextNode getNode) {
        for (int i = 0; i < length; i++) {
            char character = (char) getNode.call(value, i);
            UnsafeUtils.putChar(address + (i * 2), character);
        }
    }

    @Specialization(guards = "value==cached_value", limit = "10")
    @ExplodeLoop
    public void writeCachedEagerText(EagerText value, long address,
                                     @Cached(value = "value", allowUncached = true) EagerText cached_value,
                                     @Cached(value = "value.length()", allowUncached = true) long length) {
        for (int i = 0; i < length; i++) {
            char character = cached_value.get(i);
            UnsafeUtils.putChar(address + (i * 2), character);
        }
    }

    @Specialization(replaces = "writeCachedEagerText")
    public void writeUnCachedEagerText(StringText value, long address) {
        for (int i = 0; i < value.length(); i++) {
            char character = value.get(i);
            UnsafeUtils.putChar(address + (i * 2), character);
        }
    }


    @Specialization(replaces = "writeCachedEagerText")
    @ExplodeLoop
    public void writeUnCachedEagerText(EagerText value, long address) {
        for (int i = 0; i < value.length(); i++) {
            char character = value.get(i);
            UnsafeUtils.putChar(address + (i * 2), character);
        }
    }

    @Specialization
    public void writeEagerInt_32(Int_32 value, long address, @CachedLibrary(limit = "30") IntLibrary library) {
        UnsafeUtils.putInt(address, library.asIntValue(value));
    }


    @Specialization
    public void writeEagerInt_32(Eager_Int_64 value, long address, @CachedLibrary(limit = "30") IntLibrary library) {
        UnsafeUtils.putLong(address, library.asLongValue(value));
    }

    @Specialization
    public void writeFloat_32(Eager_Float_32 value, long address) {
        UnsafeUtils.putFloat(address, value.asFloat());
    }

    @Specialization
    public void writeFloat_32(Eager_Float_64 value, long address) {
        UnsafeUtils.putDouble(address, value.asDouble());
    }

    @Specialization
    public void writeNumeric(Numeric value, long address, @CachedLibrary(limit = "30") NumericLibrary lib) {
        UnsafeUtils.putLong(address, lib.getValue(value));
    }

    @Specialization
    public void writeChar(Char value, long address) {
        UnsafeUtils.putChar(address, value.getChar());
    }

    @Specialization
    public void writeChar(AbstractDate value, long address, @CachedLibrary(limit = "30") DateLibrary lib) {
        UnsafeUtils.putInt(address, lib.getTs(value));
    }

    @Specialization
    public void writeBoolean(Bool value, long address) {

    }

}
