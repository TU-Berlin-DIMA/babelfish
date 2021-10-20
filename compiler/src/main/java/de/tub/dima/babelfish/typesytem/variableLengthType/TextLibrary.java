package de.tub.dima.babelfish.typesytem.variableLengthType;


import com.oracle.truffle.api.library.GenerateLibrary;
import com.oracle.truffle.api.library.Library;

@GenerateLibrary()
public abstract class TextLibrary extends Library {

    public abstract char getChar(Text text, int index);


}
