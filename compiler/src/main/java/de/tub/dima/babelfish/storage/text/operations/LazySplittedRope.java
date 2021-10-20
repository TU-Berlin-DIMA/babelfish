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
public class LazySplittedRope implements TruffleObject, SplittedText {
    private final AbstractRope rope;

    public final char split;
    private int currentIndex = 0;
    private int nextStartEdge = 0;
    public final int maxLength;


    public LazySplittedRope(PointerRope rope, char split, int maxLength) {
        this.rope = rope;
        this.split = split;
        this.maxLength = maxLength;
    }

    public AbstractRope getRope() {
        return rope;
    }

    public int length() {
        return -1;
    }


    public SubstringRope get(int index) {
        return new SubstringRope(rope, 0, 0);
    }

    public static AbstractRopeBuiltins.GetChildNode getChildNode() {
        return AbstractRopeBuiltins.GetChildNode.create();
    }

    public static AbstractRopeBuiltins.GetCharTextNode getNode() {
        return AbstractRopeBuiltins.GetCharTextNode.create();
    }

    @ExportMessage
    public boolean hasArrayElements() {
        return true;
    }

    @ExportMessage
    public static class ReadArrayElement {

        @Specialization()
        public static Object readArrayElement(LazySplittedRope rope,
                                              long index,
                                              @Cached(value = "getChildNode()", allowUncached = true) AbstractRopeBuiltins.GetChildNode getChildNode,
                                              @Cached(value = "getNode()", allowUncached = true) AbstractRopeBuiltins.GetCharTextNode getCharTextNode,
                                              @Cached(value = "rope.split", allowUncached = true) char split,
                                              @Cached(value = "rope.maxLength", allowUncached = true) int maxLength

        )
                throws UnsupportedMessageException {
            PointerRope child = (PointerRope) getChildNode.call(rope);

            int start = rope.nextStartEdge;

            int end = start + 1;
            while (end < maxLength - 1 && (char) getCharTextNode.call(child, end) != split) {
                end++;
            }
            rope.nextStartEdge = end;
            rope.currentIndex = end < maxLength - 1 ? rope.currentIndex + 1 : rope.currentIndex;
            //if (end >= maxLength) {
            //    throw new RuntimeException("BOOO max:" + maxLength + " start " + start + " end:" + end + " index " + index);
            //}
            return new SubstringRope(child, start, end);
        }
    }

    @ExportMessage
    public long getArraySize() throws UnsupportedMessageException {
        return currentIndex + 1;
    }

    @ExportMessage
    public boolean isArrayElementReadable(long index) {
        return true;
    }

}
