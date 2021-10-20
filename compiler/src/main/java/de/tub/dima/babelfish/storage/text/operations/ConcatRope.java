package de.tub.dima.babelfish.storage.text.operations;

import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.storage.text.Rope;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class ConcatRope extends AbstractRope {
    public final Rope left;
    public final Rope right;

    public ConcatRope(Rope left, Rope right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public int length() {
        return left.length() + right.length();
    }

    @Override
    public char get(int index) {
        if (index < left.length()) {
            return left.get(index);
        } else {
            return right.get(index-left.length());
        }
    }

    @Override
    public boolean contains(Text otherText) {
        return false;
    }
}
