package de.tub.dima.babelfish.storage.text.leaf;

import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class CSVSourceRope extends AbstractRope {


    private final long startPosition;
    private final long endPosition;
    private final int size;



    public CSVSourceRope(long startPosition, long endPosition, int size) {
        this.startPosition = startPosition;
        this.endPosition = endPosition;
        this.size = size;
    }

    @Override
    public int length() {
        return size;
               // (int) (endPosition-startPosition);
    }

    @Override
    public char get(int index) {
        if((startPosition + index) >= endPosition){
            return '\0';
        }
        return (char) UnsafeUtils.getByte(startPosition + index);
    }

    @Override
    public boolean contains(Text otherText) {
        return false;
    }

}
