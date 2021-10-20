package de.tub.dima.babelfish.ir.pqp.nodes.records;

import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.typesytem.udt.Date;

@ExportLibrary(value = InteropLibrary.class, receiverType = Date.class)
public class LuthTypeBuiltins {


    @ExportMessage
    public static boolean hasMembers(Date type) {
        return true;
    }

    @ExportMessage
    public static boolean isMemberInvocable(Date type, String member) {
        return true;
    }

    @ExportMessage
    public static Object invokeMember(Date type, String member, Object... arguments) {
        return null;
    }

    @ExportMessage
    static Object getMembers(Date receiver, boolean includeInternal) throws UnsupportedMessageException {
        throw UnsupportedMessageException.create();
    }


}
