package de.tub.dima.babelfish.typesytem.variableLengthType;

public interface SplittedText {
    Text get(int index);
    int length();
}
