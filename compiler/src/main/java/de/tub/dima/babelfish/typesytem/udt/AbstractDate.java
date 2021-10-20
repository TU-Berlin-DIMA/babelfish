package de.tub.dima.babelfish.typesytem.udt;

import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.typesytem.LuthExportObject;
import de.tub.dima.babelfish.typesytem.valueTypes.ValueType;

import java.io.Serializable;

public abstract class AbstractDate extends LuthExportObject implements ValueType, Serializable {

    public abstract int getUnixTs();

    @ExportMessage
    public Class<?> dispatch() {
        return DateBuiltins.class;
    }

    @Override
    public String toString() {
        return String.valueOf(getUnixTs());
    }
}
