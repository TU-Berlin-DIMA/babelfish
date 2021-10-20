package de.tub.dima.babelfish.ir.lqp.udf.java;

import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.schema.field.SchemaField;
import de.tub.dima.babelfish.typesytem.valueTypes.Bool;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_64;

import java.lang.reflect.Field;

public class RecordTranslator {


    public static void setLuthFieldToTypedRecord(Record record, SchemaField schemaField, Field field, BFType value) {
        try {
            if (ReflectionHelper.isPrimitive(field)) {
                setPrimitive(record, field, value);
            } else
                ReflectionHelper.setFieldValue(field, record, value);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    private static void setPrimitive(Record object, Field field, BFType value) throws IllegalAccessException {
        Class<?> type = ReflectionHelper.getFieldType(field);
        if (type == byte.class) {
            ReflectionHelper.setFieldValue(field, object, (((Int_8) value).asByte()));
        } else if (type == short.class) {
            ReflectionHelper.setFieldValue(field, object, (((Int_16) value).asShort()));
        } else if (type == int.class) {
            ReflectionHelper.setFieldValue(field, object, (((Int_32) value).asInt()));
        } else if (type == long.class) {
            ReflectionHelper.setFieldValue(field, object, (((Int_64) value).asLong()));
        } else if (type == float.class) {
            ReflectionHelper.setFieldValue(field, object, (((Float_32) value).asFloat()));
        } else if (type == double.class) {
            ReflectionHelper.setFieldValue(field, object, (((Float_64) value).asDouble()));
        } else if (type == boolean.class) {
            ReflectionHelper.setFieldValue(field, object, (((Bool) value).getValue()));
        } else if (type == char.class) {
            ReflectionHelper.setFieldValue(field, object, (((Char) value).getChar()));
        }
    }

    public static BFType getLuthTypedValueFromField(Record record, SchemaField schemaField, Field field) {
        if (ReflectionHelper.isPrimitive(field)) {
            return boxPrimitive(record, field);
        } else {
            Object value = ReflectionHelper.getFieldValue(record, field);
            return (BFType) value;
        }
    }


    public static BFType boxPrimitive(Record record, Field field) {
        Class<?> type = ReflectionHelper.getFieldType(field);
        if (type == byte.class) {
            return new Eager_Int_8(ReflectionHelper.getByteFieldValue(record, field));
        } else if (type == short.class) {
            return new Eager_Int_16(ReflectionHelper.getShortFieldValue(record, field));
        } else if (type == int.class) {
            return new Eager_Int_32(ReflectionHelper.getIntFieldValue(record, field));
        } else if (type == long.class) {
            return new Eager_Int_64(ReflectionHelper.getLongFieldValue(record, field));
        } else if (type == float.class) {
            return new Eager_Float_32(ReflectionHelper.getFloatFieldValue(record, field));
        } else if (type == double.class) {
            return new Eager_Float_64(ReflectionHelper.getDoubleFieldValue(record, field));
        } else if (type == boolean.class) {
            return new Bool(ReflectionHelper.getBooleanFieldValue(record, field));
        } else if (type == char.class) {
            return new Char(ReflectionHelper.getCharFieldValue(record, field));
        }
        return null;
    }


}
