package de.tub.dima.babelfish.storage.layout;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.typesytem.*;
import de.tub.dima.babelfish.typesytem.record.*;
import de.tub.dima.babelfish.typesytem.schema.*;
import de.tub.dima.babelfish.typesytem.valueTypes.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.*;

import java.lang.reflect.*;

public class GenericSerializer {

    public static void addRecord(Buffer buffer) {
        buffer.getVirtualAddress().putLong(buffer.getVirtualAddress().getLong() + 1);
    }

    public static void setField(PhysicalLayout layout, Buffer buffer, int recordIndex, int fieldIndex, BFType value) {
        PhysicalField field = layout.getSchema().getField(fieldIndex);
        AddressPointer addressPointer = layout.getFieldBufferOffset(recordIndex, fieldIndex);
        AddressPointer bufferAddress = buffer.getVirtualAddress().add(addressPointer);
        field.writeValue(bufferAddress, value);
    }

    public static void setFieldToObject(Object object, Field field, BFType value) {
        try {
            if (field.getType().isPrimitive()) {
                setPrimitive(object, field, value);
            } else {
                field.set(object, value);
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    private static void setPrimitive(Object object, Field field, BFType value) throws IllegalAccessException {
        Class<?> type = field.getType();
        if (type == byte.class) {
            field.setByte(object, (((Int_8) value).asByte()));
        } else if (type == short.class) {
            field.setShort(object, (((Int_16) value).asShort()));
        } else if (type == int.class) {
            field.setInt(object, (((Int_32) value).asInt()));
        } else if (type == long.class) {
            field.setLong(object, (((Int_64) value).asLong()));
        } else if (type == float.class) {
            field.setFloat(object, (((Float_32) value).asFloat()));
        } else if (type == double.class) {
            field.setDouble(object, (((Float_64) value).asDouble()));
        } else if (type == boolean.class) {
            field.setBoolean(object, (((Bool) value).getValue()));
        } else if (type == char.class) {
            field.setChar(object, (((Char) value).getChar()));
        }
    }


    public static BFType getValueFromObject(Object object, Field field) {
        field.setAccessible(true);
        try {
            if (field.getType().isPrimitive()) {
                return getPrimitive(object, field);
            } else if (BFType.class.isAssignableFrom(field.getType())) {
                return (BFType) field.get(object);
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static BFType getPrimitive(Object object, Field field) throws IllegalAccessException {
        Class<?> type = field.getType();
        if (type == byte.class) {
            return new Eager_Int_8(field.getByte(object));
        } else if (type == short.class) {
            return new Eager_Int_16(field.getShort(object));
        } else if (type == int.class) {
            return new Eager_Int_32(field.getInt(object));
        } else if (type == long.class) {
            return new Eager_Int_64(field.getLong(object));
        } else if (type == float.class) {
            return new Eager_Float_32(field.getFloat(object));
        } else if (type == double.class) {
            return new Eager_Float_64(field.getDouble(object));
        } else if (type == boolean.class) {
            return new Bool(field.getBoolean(object));
        } else if (type == char.class) {
            return new Char(field.getChar(object));
        }
        return null;
    }


    public static void serialize(Schema lineitemSchema, PhysicalLayout physicalVariableLengthLayout, Buffer buffer, Record record) {
        PhysicalSchema schema = physicalVariableLengthLayout.getSchema();
        long index = physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer);
        for (int i = 0; i < schema.getFields().length; i++) {
            PhysicalField pf = schema.getField(i);
            AddressPointer offset = physicalVariableLengthLayout.getFieldBufferOffset(index, i);
            AddressPointer valueAddress = offset.add(buffer.getVirtualAddress());

            Field objectField = lineitemSchema.getFields()[i].getReferenceField();
            BFType type = getValueFromObject(record, objectField);
            pf.writeValue(valueAddress, type);
        }
        physicalVariableLengthLayout.incrementRecordNumber(buffer);
    }
}
