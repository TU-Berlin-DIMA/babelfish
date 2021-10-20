package de.tub.dima.babelfish.storage.text.operations;

import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.storage.text.Rope;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class ReserveRope extends AbstractRope {
    public final Rope child;
    private final int length;

    public ReserveRope(Rope child, int length) {
        this.child = child;
        this.length = length;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public char get(int index) {
        return child.get(length - index );
    }



    @Override
    public boolean contains(Text otherText) {
        return false;
    }
}
