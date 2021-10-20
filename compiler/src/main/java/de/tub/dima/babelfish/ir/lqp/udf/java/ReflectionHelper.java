package de.tub.dima.babelfish.ir.lqp.udf.java;

import de.tub.dima.babelfish.typesytem.record.Record;

import java.lang.reflect.Field;

public class ReflectionHelper {

    public static boolean isPrimitive(Field field) {
        return field.getType().isPrimitive();
    }

    public static void setFieldValue(Field field, Record record, Object object) throws IllegalAccessException {
        field.set(record, object);
    }


    public static Object getFieldValue(Record record, Field field) {
        try {
            return field.get(record);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte getByteFieldValue(Record record, Field field) {
        try {
            return field.getByte(record);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return 0;
    }


    public static short getShortFieldValue(Record record, Field field) {
        try {
            return field.getShort(record);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static int getIntFieldValue(Record record, Field field) {
        try {
            return field.getInt(record);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return 0;
    }


    public static long getLongFieldValue(Record record, Field field) {
        try {
            return field.getLong(record);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static float getFloatFieldValue(Record record, Field field) {
        try {
            return field.getFloat(record);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static double getDoubleFieldValue(Record record, Field field) {
        try {
            return field.getDouble(record);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return 0;
    }


    public static boolean getBooleanFieldValue(Record record, Field field) {
        try {
            return field.getBoolean(record);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static char getCharFieldValue(Record record, Field field) {
        try {
            return field.getChar(record);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static Class<?> getFieldType(Field field) {
        return field.getType();
    }


}
