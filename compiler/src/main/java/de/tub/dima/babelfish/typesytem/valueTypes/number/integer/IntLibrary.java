package de.tub.dima.babelfish.typesytem.valueTypes.number.integer;

import com.oracle.truffle.api.library.GenerateLibrary;
import com.oracle.truffle.api.library.Library;

@GenerateLibrary()
public abstract class IntLibrary extends Library {

    public byte asByteValue(Int value){
        throw new UnsupportedOperationException();
    }

    public short asShortValue(Int value){
        throw new UnsupportedOperationException();
    }

    public int asIntValue(Int value){
        throw new UnsupportedOperationException();
    }

    public long asLongValue(Int value){
        throw new UnsupportedOperationException();
    }

}
