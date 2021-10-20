package de.tub.dima.babelfish.typesytem.variableLengthType;

import com.oracle.truffle.api.library.DynamicDispatchLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

@ExportLibrary(value = DynamicDispatchLibrary.class)
public abstract class EagerText implements FixedLengthText{

    @ExportMessage
    public Class<?> dispatch() {
        return EagerTextBuiltins.class;
    }

}
