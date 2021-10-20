package de.tub.dima.babelfish.buildins;


import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.*;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.library.GenerateLibrary;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.ir.pqp.objects.ExecutableQuery;
import de.tub.dima.babelfish.typesytem.udt.DateBuiltins;
import de.tub.dima.babelfish.typesytem.LuthExportObject;
import de.tub.dima.babelfish.typesytem.udt.Date;

@ExportLibrary(value = InteropLibrary.class, receiverType = LuthExportObject.class)
@ImportStatic(DateBuiltins.class)
public class LuthTypeBuiltins {

    @ExportMessage
    public static boolean hasMembers(LuthExportObject type){
        return true;
    }

    @ExportMessage
    public static boolean isMemberInvocable(LuthExportObject type, String member){
        return true;
    }

    public static boolean isLuthDate(Object obj){
        return obj instanceof Date;
    }

    @ExportMessage
    public static class invokeMember{
        @Specialization(guards = "isDate")
        static Object invokeMemberDate(LuthExportObject type, String member, Object[] arguments, @Cached(value = "isLuthDate(type)", allowUncached = true) boolean isDate){
           return null;
        }

    }

    @ExportMessage
    @GenerateLibrary.Abstract
    static Object getMembers(LuthExportObject receiver, boolean includeInternal) throws UnsupportedMessageException {
        throw UnsupportedMessageException.create();
    }

    @ExportMessage
    public static boolean hasLanguage(LuthExportObject obj) {
        return true;
    }

    @ExportMessage
    public static Class<? extends TruffleLanguage<?>> getLanguage(LuthExportObject obj){
        return BabelfishEngine.class;
    }
    @ExportMessage
    public static  Object toDisplayString(LuthExportObject obj, boolean allowSideEffects) { return ""; }

}
