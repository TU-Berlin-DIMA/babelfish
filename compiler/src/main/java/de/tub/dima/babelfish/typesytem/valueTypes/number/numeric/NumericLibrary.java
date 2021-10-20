package de.tub.dima.babelfish.typesytem.valueTypes.number.numeric;

import com.oracle.truffle.api.library.GenerateLibrary;
import com.oracle.truffle.api.library.Library;

@GenerateLibrary()
public abstract class NumericLibrary extends Library {

    public abstract long getValue(Numeric value);

}
