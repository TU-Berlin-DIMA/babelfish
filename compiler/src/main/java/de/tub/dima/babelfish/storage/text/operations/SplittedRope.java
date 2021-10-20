package de.tub.dima.babelfish.storage.text.operations;

import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.buildins.AbstractRopeBuiltins;
import de.tub.dima.babelfish.storage.text.leaf.PointerRope;
import de.tub.dima.babelfish.typesytem.variableLengthType.SplittedText;

@ExportLibrary(value = InteropLibrary.class)
public class SplittedRope implements TruffleObject, SplittedText {
    private final AbstractRope rope;

    private final int[] indexArray;
    private final char split;
    private int maxIndex;
    private int next = 1;


    public SplittedRope(PointerRope rope, char split, int[] array, int c) {
        this.rope = rope;
        this.split = split;
        this.indexArray = array;
        this.maxIndex = c;
    }

    public AbstractRope getRope() {
        return rope;
    }

    public int length() {
        return maxIndex;
    }


    public SubstringRope get(int index) {
        return new SubstringRope(rope, indexArray[(int) index], indexArray[(int) index + 1]);
    }

    @ExportMessage
    public boolean hasArrayElements() {
        return true;
    }


    public static AbstractRopeBuiltins.GetChildNode getChildNode() {
        return AbstractRopeBuiltins.GetChildNode.create();
    }


    public static AbstractRopeBuiltins.GetCharTextNode getNode() {
        return AbstractRopeBuiltins.GetCharTextNode.create();
    }


    @ExportMessage
    public static class readArrayElement {

        @Specialization()
        public static Object readArrayElement(SplittedRope rope,
                                              long index,
                                              @Cached(value = "getChildNode()", allowUncached = true) AbstractRopeBuiltins.GetChildNode getChildNode) throws UnsupportedMessageException {
            PointerRope child = (PointerRope) getChildNode.call(rope);
            return new SubstringRope(child, rope.indexArray[(int) index], rope.indexArray[(int) index + 1]);
        }
    }

    @ExportMessage
    public long getArraySize() throws UnsupportedMessageException {
        return maxIndex;
    }

    @ExportMessage
    public boolean isArrayElementReadable(long index) {
        return true;
    }

}
