package de.tub.dima.babelfish.storage.layout.fields;

import com.oracle.truffle.api.nodes.ExplodeLoop;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.typesytem.variableLengthType.FixedLengthText;
import de.tub.dima.babelfish.typesytem.variableLengthType.SplittedText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class NativeFixedText implements FixedLengthText {

    private final long address;

    private final int size;

    private final static int VALUE_SIZE = 2;


    public NativeFixedText(long address, int size) {
        this.size = size;
        this.address = address;
    }

    @Override
    public int length() {
        return size;
    }

    @Override
    public char get(int index) {
        return UnsafeUtils.getChar(address + (index * VALUE_SIZE));
    }

    @Override
    public Text substring(int start, int end) {
        return null;
    }

    @Override
    public boolean equals(Text otherText) {
        return false;
    }

    @Override
    public boolean contains(Text otherText) {
        return false;
    }

    @Override
    public Text concat(Text otherText) {
        return null;
    }

    @Override
    public Text lowercase() {
        return null;
    }

    @Override
    public Text uppercase() {
        return null;
    }

    @Override
    public Text reverse() {
        return null;
    }

    @Override
    public SplittedText split(char split) {
        return null;
    }

    @Override
    @ExplodeLoop
    public boolean equals(Object other) {
        Text otherText = (Text) other;
        int minSize = (Math.min(length(), otherText.length()));
        boolean result = true;
        for (int i = 0; i < minSize; i++) {
            result = result && get(i) == otherText.get(i);
            //    return false;
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder(length());
        for (int i = 0; i < size; i++) {
            stringBuilder.append(get(i));
        }
        return stringBuilder.toString();
    }
}
