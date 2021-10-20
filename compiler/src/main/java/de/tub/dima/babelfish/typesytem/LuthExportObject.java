package de.tub.dima.babelfish.typesytem;

import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.DynamicDispatchLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.buildins.LuthTypeBuiltins;

@ExportLibrary(value = DynamicDispatchLibrary.class )
public class LuthExportObject implements TruffleObject {
    @ExportMessage
    public Class<?> dispatch() {
        return LuthTypeBuiltins.class;
    }


}
