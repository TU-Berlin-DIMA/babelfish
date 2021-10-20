package de.tub.dima.babelfish.ir.pqp.nodes.types;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.RootNode;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.storage.text.leaf.PointerRope;
import de.tub.dima.babelfish.typesytem.variableLengthType.FixedLengthText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;
import de.tub.dima.babelfish.typesytem.variableLengthType.TextLibrary;

@NodeChild(value = "first", type = ArgumentReadNode.class)
@NodeChild(value = "second", type = ArgumentReadNode.class)
public abstract class TextEqualNode extends RootNode {

    protected TextEqualNode(TruffleLanguage<?> language) {
        super(language);
    }

    public static TextEqualNode create(TruffleLanguage<?> language) {
        return TextEqualNodeGen.create(language, new ArgumentReadNode(0), new ArgumentReadNode(1));
    }


    @ExplodeLoop
    public boolean equalsLoop(FixedLengthText first, FixedLengthText second, int size) {
        boolean returnValue = true;
        for (int i = 0; i < size; i++) {
            returnValue = returnValue && first.get(i) == second.get(i);
        }
        return returnValue;
    }

    @Specialization
    public boolean equals(FixedLengthText first, FixedLengthText second,
                          @Cached(value = "first.length()", allowUncached = true) int firstLength, @Cached(value = "second.length()", allowUncached = true) int secondLength) {
        if (firstLength != secondLength)
            return false;

        return equalsLoop(first, second, firstLength);
    }

    @Specialization
    @ExplodeLoop
    public boolean equals(PointerRope first, PointerRope second, @Cached(value = "first.length()", allowUncached = true) int firstLength) {

        for (int i = 0; i < firstLength; i++) {
            if (!(first.get(i) == second.get(i)))
                return false;
        }
        return true;
    }

    @Specialization
    @ExplodeLoop
    public boolean equals(Text first, Text second,
                          @Cached(value = "first.length()", allowUncached = true) int firstLength,
                          @Cached(value = "second.length()", allowUncached = true) int secondLength,
                          @CachedLibrary(limit = "30") TextLibrary leftTextLib,
                          @CachedLibrary(limit = "30") TextLibrary secondTextLib
    ) {
        if (firstLength != secondLength)
            return false;

        boolean returnValue = true;
        for (int i = 0; i < firstLength; i++) {
            returnValue = returnValue && leftTextLib.getChar(first, i) == leftTextLib.getChar(second, i);
        }
        return returnValue;
    }
}
