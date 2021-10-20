package de.tub.dima.babelfish.typesytem.valueTypes;

import java.util.Objects;

public class Char implements ValueType {

    private final char value;

    public Char(char value) {
        this.value = value;
    }

    public char getChar(){
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Char aChar = (Char) o;
        return value == aChar.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
