package de.tub.dima.babelfish.storage.text.leaf;

import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;
import org.apache.arrow.memory.util.ArrowBufPointer;

public class ArrowSourceRope extends AbstractRope {

    private final int size;
    private final ArrowBufPointer vector;


    public ArrowSourceRope(ArrowBufPointer vector, int size) {
        this.vector = vector;
        this.size = size;
    }

    @Override
    public int length() {
        return size;
    }

    @Override
    public char get(int index) {
        if(index >= vector.getLength())
            return '\0';
        long offset = vector.getOffset() + index;
        return (char) vector.getBuf().getByte(offset);
    }

    @Override
    public boolean contains(Text otherText) {
        return false;
    }

}
