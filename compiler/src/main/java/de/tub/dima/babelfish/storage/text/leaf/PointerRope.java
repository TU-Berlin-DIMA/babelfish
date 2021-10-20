package de.tub.dima.babelfish.storage.text.leaf;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.storage.text.operations.SplittedRope;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class PointerRope extends AbstractRope {

    private final long address;
    @CompilerDirectives.CompilationFinal
    private final int size;

    private final static long VALUE_SIZE = 2;

    public PointerRope(long address, int size) {
        this.address = address;
        this.size = size;
    }

    @Override
    public int length() {
        return size;
    }

    @Override
    public char get(int index) {
        return UnsafeUtils.getChar(address + (index * VALUE_SIZE));
    }

    public long getAsLong(int index) {
        return UnsafeUtils.getLong(address + index * VALUE_SIZE);
    }

    @ExplodeLoop
    public SplittedRope split(char split) {
        PointerRope r = (PointerRope) this;
        int[] array = new int[128];
        int c = 0;
        for (int i = 0; i < size; i++) {
            if ((char) get(i) == split) {
                c++;
                array[c] = i;
            }
        }
        return new SplittedRope((PointerRope) this, split, array, c);
    }

    @Override
    public boolean contains(Text otherText) {
        return false;
    }

}
