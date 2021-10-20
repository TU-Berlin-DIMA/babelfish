package de.tub.dima.babelfish.storage.text.operations;

import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class UppercaseRope extends AbstractRope {
    public final AbstractRope rope;

    public UppercaseRope(AbstractRope abstractRope) {
        this.rope = abstractRope;
    }

    @Override
    public int length() {
        return rope.length();
    }

    @Override
    public char get(int index) {
        return (char) (rope.get(index) & (char)0x5f);
    }

    @Override
    public boolean contains(Text otherText) {
        return false;
    }
}
