package de.tub.dima.babelfish.typesytem.variableLengthType;

import com.oracle.truffle.api.CompilerDirectives;


public class StringText extends EagerText {

    private final String text;

    public StringText(String chars) {
        this.text = chars;
    }

    public StringText(String chars, int size) {
        StringBuilder sb = new StringBuilder(size);
        sb.insert(0, chars);
        sb.setLength(size);
        this.text = sb.toString();
    }

    @Override
    public int length() {
        return text.length();
    }

    @Override
    public boolean equals(Object otherText) {
        if (otherText instanceof StringText) {
            return text.equals(((StringText) otherText).text);
        } else if (otherText instanceof String) {
            return text.equals(((String) otherText));
        }
        return false;
    }

    @Override
    public char get(int index) {
        return text.charAt(index);
    }

    @Override
    public Text substring(int start, int end) {
        return new StringText(this.text.substring(start, end), end - start);
    }

    @Override
    public boolean equals(Text otherText) {
        if (otherText instanceof StringText) {
            return text.equals(((StringText) otherText).text);
        }
        return false;
    }

    @Override
    public boolean contains(Text otherText) {
        return false;
    }

    @Override
    public Text concat(Text otherText) {
        return new StringText(this.text + otherText.toString());
    }

    @Override
    @CompilerDirectives.TruffleBoundary
    public Text lowercase() {
        return new StringText(text.toLowerCase(), length());
    }

    @Override
    @CompilerDirectives.TruffleBoundary
    public Text uppercase() {
        return new StringText(text.toUpperCase(), length());
    }

    @Override
    public Text reverse() {
        StringBuilder input1 = new StringBuilder();

        // append a string into StringBuilder input1
        input1.append(this.text);

        // reverse StringBuilder input1
        return new StringText(input1.reverse().toString());
    }

    @Override
    public SplittedText split(char split) {
        return null;
    }

    @Override
    public String toString() {
        return text;
    }


}
