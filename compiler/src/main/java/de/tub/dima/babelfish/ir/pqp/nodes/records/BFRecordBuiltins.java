package de.tub.dima.babelfish.ir.pqp.nodes.records;

import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.typesytem.BFType;

@ExportLibrary(value = BFRecordLibrary.class, receiverType = BFRecord.class)
public class BFRecordBuiltins {

    static public int getFieldIndex(BFRecord object, String name) {
        if (!object.getObjectSchema().containsField(name)) {
            object.getObjectSchema().addField(name);
        }
        return object.getObjectSchema().getFieldIndexFromConstant(name);
    }

    @ExportMessage
    public static class SetValue {
        @Specialization
        public static void setValue(BFRecord object,
                                    String fieldName,
                                    BFType value,
                                    @Cached(value = "getFieldIndex(object, fieldName)", allowUncached = true) int index) {
            object.setValue(index, value);
        }
    }

    @ExportMessage
    public static class GetValue {
        @Specialization
        public static BFType getValue(BFRecord object,
                                      String fieldName,
                                      @Cached(value = "getFieldIndex(object, fieldName)", allowUncached = true) int index) {
            return object.getValue(index);
        }
    }

}
