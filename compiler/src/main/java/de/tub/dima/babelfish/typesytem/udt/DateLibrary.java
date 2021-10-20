package de.tub.dima.babelfish.typesytem.udt;


import com.oracle.truffle.api.library.GenerateLibrary;
import com.oracle.truffle.api.library.Library;

@GenerateLibrary()
public abstract class DateLibrary extends Library {

    public abstract int getTs(AbstractDate date);
    public abstract int getYear(AbstractDate date);


}
