package de.tub.dima.babelfish.storage.text.operations;

import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.storage.text.Rope;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class SubstringRope extends AbstractRope {

    public final int start;
    public final int end;
    public final Rope child;

    public SubstringRope(Rope child, int start, int end) {
        this.start = start;
        this.end = end;
        this.child = child;
    }

    @Override
    public int length() {
        return end - start;
    }

    @Override
    public char get(int index) {
        return child.get(start + index);
    }

    @Override
    public boolean contains(Text otherText) {
        return false;
    }


}
