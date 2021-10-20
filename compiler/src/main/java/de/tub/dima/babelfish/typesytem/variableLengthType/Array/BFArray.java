package de.tub.dima.babelfish.typesytem.variableLengthType.Array;

import com.oracle.truffle.api.library.DynamicDispatchLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.typesytem.variableLengthType.VariableLengthType;

@ExportLibrary(DynamicDispatchLibrary.class)
public abstract class BFArray implements VariableLengthType {

    private final int length;

    protected BFArray(int length) {
        this.length = length;
    }

    public int getLength() {
        return length;
    }

    public abstract ComponentTypes getComponentType();

    @ExportMessage
    public Class<?> dispatch() {
        return ArrayBuiltins.class;
    }

}
