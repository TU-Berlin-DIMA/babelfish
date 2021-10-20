package de.tub.dima.babelfish.storage.text;

import com.oracle.truffle.api.CompilerDirectives;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.typesytem.variableLengthType.*;

public class PointerText extends EagerText {

    private final long address;
    @CompilerDirectives.CompilationFinal
    private final int size;

    private final static int VALUE_SIZE = 2;

    public PointerText(long address, int size) {
        this.address = address;
        this.size = size;
    }

    @Override
    public int length() {
        return size;
    }

    @Override
    public char get(int index) {
        return UnsafeUtils.getChar(address + index * VALUE_SIZE);
    }

    @Override
    public Text substring(int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++) {
            sb.append(get(i));
        }
        return new StringText(sb.toString());
    }

    @Override
    public boolean equals(Text otherText) {
        if (length() == otherText.length()) {
            for (int i = 0; i < otherText.length(); i++) {
                if (get(i) != otherText.get(i)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean contains(Text otherText) {
        return false;
    }

    @Override
    public Text concat(Text otherText) {
        return new StringText(this.toString() + otherText.toString());
    }

    @Override
    public Text lowercase() {
       return new StringText(toString().toLowerCase());
    }

    @Override
    public Text uppercase() {
        return new StringText(toString().toUpperCase());
    }

    @Override
    public Text reverse() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(get(i));
        }
        return new StringText(sb.reverse().toString());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(get(i));
        }
        return sb.toString();
    }

    @Override
    public SplittedText split(char split) {
        String[] array = this.toString().split(split+"");
        StringText[] res = new StringText[array.length];
        for (int i = 0; i < res.length; i++) {
            res[i] = new StringText(array[i]);
        }
        return new StringTextArray(res);
    }
}
