package de.tub.dima.babelfish.ir.pqp.objects.records;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.*;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.Bool;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Eager_Int_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Lazy_Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;
import de.tub.dima.babelfish.BabelfishEngine;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;

import java.util.Arrays;

/**
 * The BFRecord is the central Common Data Representation CDR of BabelfishEngine.
 * It represents all records between operators.
 */
@ExportLibrary(value = InteropLibrary.class)
public final class BFRecord extends DynamicRecord implements TruffleObject {

    /**
     * The values arrays stores the values of BFRecords.
     * During compilation, BabelfishEngine bindes each value to a fixed data type.
     */
    @CompilerDirectives.CompilationFinal(dimensions = 1)
    private final Object[] values;

    /**
     * The record schema describes the values in the BFRecord.
     * It is is assumed to be compile time constant.
     */
    @CompilerDirectives.CompilationFinal
    private final RecordSchema recordSchema;


    public BFRecord(RecordSchema schema, Object[] values) {
        this.recordSchema = schema;
        this.values = values;
    }

    public boolean isAdoptable() {
        return true;
    }

    @Override
    public void setValue(String key, BFType value) {
        if (CompilerDirectives.inInterpreter())
            this.recordSchema.addField(key);

        int index = recordSchema.getFieldIndexFromConstant(key);
        values[index] = value;
    }

    public void setValue(int index, BFType value) {
        values[index] = value;
    }

    public RecordSchema getObjectSchema() {
        return recordSchema;
    }

    @Override
    public <T extends BFType> T getValue(String key) {
        int index = this.recordSchema.getFieldIndex(key);
        return (T) values[index];
    }

    public <T extends BFType> T getValue(Object[] values, int index) {
        return (T) values[index];
    }


    public <T extends BFType> T getValue(int index) {
        return getValue(this.values, index);
    }

    public <T extends BFType> T getValueWithClass(Object[] values, int index, Class clazz) {
        return (T) values[index];
    }


    public <T extends BFType> T getValue(int index, Class<T> clazz) {
        return getValueWithClass(this.values, index, clazz);
    }

    public static BFRecord createObject(RecordSchema recordSchema) {
        return new BFRecord(recordSchema, new Object[32]);
    }

    public BFRecord copy(RecordSchema recordSchema) {
        return new BFRecord(recordSchema, values);
    }



    @ExportMessage
    public boolean hasMembers() {
        return true;
    }

    @ExportMessage
    static class IsMemberReadable {

        @Specialization
        public static boolean isMemberReadable(BFRecord object, String member, @Cached(value = "getIndex(object, member)", allowUncached = true) int index) {
            return index != -1;
        }

    }


    @ExportMessage
    public boolean isMemberModifiable(String member) {
        return true;
    }

    @ExportMessage
    boolean isMemberInsertable(String member) {
        return true;
    }

    public static int getIndex(BFRecord object, String member) {
        return object.recordSchema.getFieldIndex(member);
    }

    public static int getIndexAndAdd(BFRecord object, String member) {
        if (!object.recordSchema.containsField(member))
            object.recordSchema.addField(member);
        return object.recordSchema.getFieldIndex(member);
    }

    public static StringText getStringText(String string) {
        return new StringText(string, string.length());
    }

    @ExportMessage
    static class WriteMember {
        @Specialization(guards = "value==cached_value")
        public static void writeMemberCached(BFRecord object, String member, String value,
                                             @Cached(value = "value", allowUncached = true) String cached_value,
                                             @Cached(value = "getStringText(value)", allowUncached = true) StringText stringText,
                                             @Cached(value = "getIndexAndAdd(object, member)", allowUncached = true) int index) throws UnsupportedMessageException, UnknownIdentifierException, UnsupportedTypeException {
            object.setValue(index, stringText);
        }

        @Specialization(replaces = "writeMemberCached")
        public static void writeMember(BFRecord object, String member, String value,
                                       @Cached(value = "getIndexAndAdd(object, member)", allowUncached = true) int index) throws UnsupportedMessageException, UnknownIdentifierException, UnsupportedTypeException {
            object.setValue(index, new StringText(value));
        }

        @Specialization()
        public static void writeMember(BFRecord object, String member, BFType value, @Cached(value = "getIndexAndAdd(object, member)", allowUncached = true) int index) throws UnsupportedMessageException, UnknownIdentifierException, UnsupportedTypeException {
            object.setValue(index, value);
        }

        @Specialization()
        public static void writeMember(BFRecord object, String member, double value, @Cached(value = "getIndexAndAdd(object, member)", allowUncached = true) int index) throws UnsupportedMessageException, UnknownIdentifierException, UnsupportedTypeException {
            object.setValue(index, new Eager_Float_64(value));
        }


        @Specialization()
        public static void writeMember(BFRecord object,
                                       String member, int value,
                                       @Cached(value = "getIndexAndAdd(object, member)", allowUncached = true) int index
        ) {
            object.setValue(index, new Eager_Int_32(value));
        }

        @Specialization()
        public static void writeMember(BFRecord object,
                                       String member, long value,
                                       @Cached(value = "getIndexAndAdd(object, member)", allowUncached = true) int index) {
            object.setValue(index, new Eager_Int_64(value));
        }
    }

    @ExportMessage
    static class ReadMember {


        public static boolean isFloat32(BFRecord object, int index) {
            return object.values[index] instanceof Float_32;
        }

        public static boolean isFloat64(BFRecord object, int index) {
            return object.values[index] instanceof Float_64;
        }

        public static boolean isEagerInt32(BFRecord object, int index) {
            return object.values[index] instanceof Eager_Int_32;
        }

        public static boolean isLazyInt32(BFRecord object, int index) {
            return object.values[index] instanceof Lazy_Int_32;
        }

        public static boolean isInt64(BFRecord object, int index) {
            return object.values[index] instanceof Int_64;
        }

        public static boolean isText(BFRecord object, int index) {
            return object.values[index] instanceof Text;
        }

        public static boolean isDate(BFRecord object, int index) {
            return object.values[index] instanceof Date;
        }

        public static boolean isBoolean(BFRecord object, int index) {
            return object.values[index] instanceof Bool;
        }

        public static boolean isNumeric(BFRecord object, int index) {
            return object.values[index] instanceof Numeric;
        }

        @Specialization(guards = "isFloat32")
        public static float readFloat32(BFRecord object, String member,
                                        @Cached(value = "getIndex(object, member)", allowUncached = true) int index,
                                        @Cached(value = "isFloat32(object,index)", allowUncached = true) boolean isFloat32) {
            Float_32 value = (Float_32) object.getValue(index);
            return ((Float_32) value).asFloat();
        }

        @Specialization(guards = "isBoolean")
        public static boolean readBoolean(BFRecord object, String member,
                                          @Cached(value = "getIndex(object, member)", allowUncached = true) int index,
                                          @Cached(value = "isBoolean(object,index)", allowUncached = true) boolean isBoolean) {
            Bool value = (Bool) object.getValue(index);
            return ((Bool) value).getValue();
        }

        @Specialization(guards = "isFloat64")
        public static double readFloat64(BFRecord object, String member,
                                         @Cached(value = "getIndex(object, member)", allowUncached = true) int index,
                                         @Cached(value = "isFloat64(object,index)", allowUncached = true) boolean isFloat64) {
            Float_64 value = (Float_64) object.getValue(index);
            return ((Float_64) value).asDouble();
        }

        public static double getDivisor(BFRecord object, int index) {
            Numeric value = (Numeric) object.getValue(index);
            return Numeric.numericShifts[value.getPrecision()];
        }

        @Specialization(guards = "isNumeric")
        public static Numeric readNumeric(BFRecord object, String member,
                                          @Cached(value = "getIndex(object, member)", allowUncached = true) int index,
                                          @Cached(value = "isNumeric(object,index)", allowUncached = true) boolean isNumeric,
                                          @Cached(value = "getDivisor(object,index)", allowUncached = true) double divisor) {
            return ((Numeric) object.getValue(index));
        }

        @Specialization(guards = "isEagerInt32")
        public static int readInt32(BFRecord object, String member,
                                    @Cached(value = "getIndex(object, member)", allowUncached = true) int index,
                                    @Cached(value = "isEagerInt32(object,index)", allowUncached = true) boolean isEagerInt32) {
            Eager_Int_32 value = (Eager_Int_32) object.getValue(index);
            return (value).asInt();
        }

        @Specialization(guards = "isLazyInt32")
        public static int readLazyInt32(BFRecord object, String member,
                                        @Cached(value = "getIndex(object, member)", allowUncached = true) int index,
                                        @Cached(value = "isLazyInt32(object,index)", allowUncached = true) boolean isLazyInt32) {
            Lazy_Int_32 value = CompilerDirectives.castExact(object.getValue(index), Lazy_Int_32.class);
            return (value).asInt();
        }

        @Specialization(guards = "isInt64")
        public static long readInt64(BFRecord object, String member,
                                     @Cached(value = "getIndex(object, member)", allowUncached = true) int index,
                                     @Cached(value = "isInt64(object,index)", allowUncached = true) boolean isInt64) {
            Int_64 value = (Int_64) object.getValue(index);
            return (value).asLong();
        }

        @Specialization(guards = "isDate")
        public static Date readDate(BFRecord object, String member,
                                    @Cached(value = "getIndex(object, member)", allowUncached = true) int index,
                                    @Cached(value = "isDate(object,index)", allowUncached = true) boolean isDate) {
            return (Date) object.getValue(index);
        }

        public static boolean naiveStringHandling() {
            return RuntimeConfiguration.NAIVE_STRING_HANDLING;
        }

        @Specialization(guards = "isText")
        public static Object readText(BFRecord object, String member,
                                      @Cached(value = "getIndex(object, member)", allowUncached = true) int index,
                                      @Cached(value = "isText(object,index)", allowUncached = true) boolean isText,
                                      @Cached(value = "naiveStringHandling()", allowUncached = true) boolean naiveStringHandling) {
            if (!naiveStringHandling) {
                return object.values[index];
            } else {
                Text text = (Text) object.getValue(index);
                return text.toString();
            }
        }

        @Specialization()
        public static Object readLuthType(BFRecord object, String member,
                                          @Cached(value = "getIndex(object, member)", allowUncached = true) int index) {
            return object.getValue(index);
        }
    }

    @ExportMessage
    public Object getMembers(boolean includeInternal) {
        return this.recordSchema.fieldNames;
    }

    @ExportMessage
    public boolean hasLanguage() {
        return true;
    }

    @ExportMessage
    public Class<? extends TruffleLanguage<?>> getLanguage(){
        return BabelfishEngine.class;
    }

    @ExportMessage
    Object toDisplayString(boolean allowSideEffects) {
        return Arrays.toString(this.values);
    }

    @Override
    public String toString() {
        return "BFRecord{" +
                "values=" + Arrays.toString(values) +
                ", recordSchema=" + recordSchema +
                '}';
    }
}
