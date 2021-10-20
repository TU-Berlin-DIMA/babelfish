package de.tub.dima.babelfish.storage.text.operations;

import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.storage.text.Rope;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class LowercaseRope extends AbstractRope {
    public final Rope rope;

    public LowercaseRope(Rope child) {
        this.rope = child;
    }

    @Override
    public int length() {
        return rope.length();
    }

    @Override
    public char get(int index) {
        // to lowercase
        return (char)(rope.get(index) ^ 0x20);
    }

    @Override
    public boolean contains(Text otherText) {
        return false;
    }
}
