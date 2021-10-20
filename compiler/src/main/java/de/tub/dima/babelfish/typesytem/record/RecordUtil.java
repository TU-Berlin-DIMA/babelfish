package de.tub.dima.babelfish.typesytem.record;

import de.tub.dima.babelfish.typesytem.*;
import de.tub.dima.babelfish.typesytem.schema.*;
import de.tub.dima.babelfish.typesytem.schema.field.*;
import de.tub.dima.babelfish.typesytem.udt.*;
import de.tub.dima.babelfish.typesytem.valueTypes.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Precision;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.BFArray;
import de.tub.dima.babelfish.typesytem.variableLengthType.*;

import java.lang.reflect.*;

public class RecordUtil {

    public static Field[] getFields(Class recordClass) {
        return recordClass.getFields();
    }

    public static Schema createSchema(Class<? extends Record> recordClass) throws SchemaExtractionException {
        SchemaBuilder schemaBuilder = SchemaBuilder.createBuilder(recordClass);
        for (Field field : getFields(recordClass)) {
            SchemaField sf = getSchemaField(field, null);
            schemaBuilder.addField(sf);
        }
        return schemaBuilder.build();
    }

    public static Schema createSchema(Class<? extends Record> recordClass, Type parameterizedType) throws SchemaExtractionException {
        SchemaBuilder schemaBuilder = SchemaBuilder.createBuilder(recordClass);
        for (Field field : getFields(recordClass)) {
            SchemaField sf = getSchemaField(field,  parameterizedType);
            schemaBuilder.addField(sf);
        }
        return schemaBuilder.build();
    }

    public static Schema createSchemaFromUDT(Class<? extends UDT> udtClass) throws SchemaExtractionException {
        SchemaBuilder schemaBuilder = SchemaBuilder.createBuilder();
        for (Field field : getFields(udtClass)) {
            SchemaField sf = getSchemaField(field, null);
            schemaBuilder.addField(sf);
        }
        return schemaBuilder.build();
    }

    public static SchemaField getSchemaField(Field field, Type parameterizedType) throws SchemaExtractionException {
        Class<?> fieldType = field.getType();
        if (fieldType == Object.class) {
            if (parameterizedType == null)
                throw new SchemaExtractionException("Generic field but no generic info provided");

            TypeVariable<? extends Class<?>>[] typePara = field.getDeclaringClass().getTypeParameters();
            TypeVariable tvar = (TypeVariable) field.getGenericType();
            for (int i = 0; i < typePara.length; i++) {
                if (typePara[i] == tvar) {
                    fieldType = (Class<?>) ((ParameterizedType) parameterizedType).getActualTypeArguments()[i];
                }
            }
        }

        if (isLuthValueType(fieldType)) {
            if (isLuthNumberType(fieldType)) {
                return getNumberField(field, fieldType);
            } else if (isBoolean(fieldType)) {
                return new ValueField(getName(field), Bool.class, getIndexOfField(field), isNeverNull(field), field);
            } else if (isCharacter(fieldType)) {
                return new ValueField(getName(field), Char.class, getIndexOfField(field), isNeverNull(field), field);
            }else if(isLuthNumeric(fieldType)){
                return getNumericField(field, fieldType);
            }else if(isDate(fieldType)){
                return getDateField(field, fieldType);
            }
        } else if (isLuthVariableLengthType(fieldType)) {
            if (isArray(fieldType)) {
                return getArrayField(field, fieldType);
            } else if(isTextField(fieldType)) {
                return getTextField(field, fieldType);
            }
        } else if (isLuthRecord(fieldType)) {
            return new RecordField(getName(field), (Class<Record>) fieldType, getIndexOfField(field), isNeverNull(field));
        } else if (isLuthUDT(fieldType)) {
            return new UDTField(getName(field), (Class<UDT>) fieldType, getIndexOfField(field), isNeverNull(field), field);
        }

        throw new SchemaExtractionException("Schema can not be generated for field " + field + " and type " + parameterizedType);
    }

    private static SchemaField getDateField(Field field, Class<?> fieldType) {
        return new DateField(getName(field), getIndexOfField(field), (Class<BFType>) fieldType, isNeverNull(field),field );

    }

    private static boolean isDate(Class<?> clazz) {
        return AbstractDate.class.isAssignableFrom(clazz);
    }

    private static SchemaField getNumericField(Field field, Class<?> fieldType) {
        int precision = field.getAnnotation(Precision.class).value();
        return new NumericField(getName(field), getIndexOfField(field), (Class<BFType>) fieldType, isNeverNull(field),field, precision );
    }


    private static boolean isTextField(Class<?> fieldType) {
        return Text.class.isAssignableFrom(fieldType);
    }

    private static SchemaField getTextField(Field field, Class<?> fieldType) {
        if (field.isAnnotationPresent(MaxLength.class)) {
            long maxLength = field.getAnnotation(MaxLength.class).length();
            return new TextField(getName(field), (Class<BFType>) fieldType, getIndexOfField(field), isNeverNull(field),field, maxLength );
        }
        return new TextField(getName(field), (Class<BFType>) fieldType, getIndexOfField(field), isNeverNull(field), field);
    }

    public static boolean isLuthVariableLengthType(Class<?> fieldType) {
        return VariableLengthType.class.isAssignableFrom(fieldType) || fieldType.isArray();
    }

    public static boolean isArray(Class<?> fieldType) {
        return BFArray.class.isAssignableFrom(fieldType) || fieldType.isArray();
    }

    public static ArrayField getArrayField(Field field, Class<?> fieldType) {
        Type type = field.getGenericType();
        long maxLength = 0;
        if (field.isAnnotationPresent(MaxLength.class)) {
            maxLength = field.getAnnotation(MaxLength.class).length();
        }

        if (field.getType().isArray()) {
            Class<? extends BFType> componentType = getType(fieldType.getComponentType());
            int dimensions = getArrayDimension(fieldType.getComponentType());
            return new ArrayField(field, getName(field), BFArray.class, getIndexOfField(field), isNeverNull(field), componentType, dimensions, maxLength);
        } else {
            int dimensions = getArrayDimension(field.getGenericType());
            Class<?> subtype = getGenericComponentType(type);
            return new ArrayField(field, getName(field), (Class<BFType>) fieldType, getIndexOfField(field), isNeverNull(field), (Class<BFType>) subtype, dimensions, maxLength);
        }
    }

    private static int getArrayDimension(Class<?> type) {
        if (type.isArray())
            return getArrayDimension(type.getComponentType()) + 1;
        return 1;
    }

    private static int getArrayDimension(Type type) {
        if (type instanceof ParameterizedType) {
            Type subType = ((ParameterizedType) type).getActualTypeArguments()[0];
            return getArrayDimension(subType) + 1;
        }
        return 1;
    }

    private static Class<?> getGenericComponentType(Type type) {
        if (type instanceof ParameterizedType) {
            Type subType = ((ParameterizedType) type).getActualTypeArguments()[0];
            return getGenericComponentType(subType);
        }
        return (Class<?>) type;
    }


    public static boolean isLuthType(Class<?> clazz) {
        return BFType.class.isAssignableFrom(clazz);
    }

    public static boolean isLuthRecord(Class<?> clazz) {
        return clazz.isAnnotationPresent(LuthRecord.class) || Record.class.isAssignableFrom(clazz);
    }

    public static boolean isLuthValueType(Class<?> clazz) {
        return clazz.isPrimitive() || isLuthNumberType(clazz) || ValueType.class.isAssignableFrom(clazz);
    }

    public static boolean isLuthNumberType(Class<?> clazz) {
        return (clazz.isPrimitive() && !isBoolean(clazz) && !isCharacter(clazz)) || Number.class.isAssignableFrom(clazz) || NumberType.class.isAssignableFrom(clazz);
    }

    public static boolean isLuthNumeric(Class<?> clazz) {
        return Numeric.class.isAssignableFrom(clazz);
    }


    public static boolean isBoolean(Class<?> clazz) {
        return Boolean.class.isAssignableFrom(clazz) || Bool.class.isAssignableFrom(clazz);
    }

    public static boolean isCharacter(Class<?> clazz) {
        return Character.class.isAssignableFrom(clazz) || Char.class.isAssignableFrom(clazz);
    }

    private static NumberField getNumberField(Field field, Class<?> fieldType) {
        if (field.isAnnotationPresent(ValueRange.class)) {
            long maxValue = field.getAnnotation(ValueRange.class).maxValue();
            long minValue = field.getAnnotation(ValueRange.class).minValue();
            return new NumberField(getName(field), getIndexOfField(field), (Class<NumberType>) getType(fieldType), isNeverNull(field), minValue, maxValue, field);
        } else {
            Class<NumberType> type = (Class<NumberType>) getType(fieldType);
            //try {
            //    Number min = (Number) type.getFields()[0].get(null);
            //    Number max = (Number) type.getFields()[1].get(null);
            //    return new NumberField(getName(field), getIndexOfField(field), (Class<NumberType>) getType(fieldType), isNeverNull(field), min, max);
            //} catch (IllegalAccessException e) {
            //    e.printStackTrace();
            //}
            return new NumberField(getName(field), getIndexOfField(field), (Class<NumberType>) getType(fieldType), isNeverNull(field), 0, 0, field);
        }
    }

    public static Class<? extends BFType> getType(Class<?> type) {
        if (type.isPrimitive()) {
            return getTypeFromPrimitive(type);
        } else {
            return (Class<BFType>) type;
        }
    }

    public static Class<? extends BFType> getTypeFromPrimitive(Class<?> type) {
        if (type == byte.class) {
            return Int_8.class;
        } else if (type == short.class) {
            return Int_16.class;
        } else if (type == int.class) {
            return Int_32.class;
        } else if (type == long.class) {
            return Int_64.class;
        } else if (type == float.class) {
            return Float_32.class;
        } else if (type == double.class) {
            return Float_64.class;
        } else if (type == boolean.class) {
            return Bool.class;
        } else if (type == char.class) {
            return Char.class;
        }
        return null;
    }

    private static boolean isNeverNull(Field field) {
        return field.isAnnotationPresent(FieldNeverNull.class);
    }

    public static boolean isLuthUDT(Class<?> clazz) {
        return UDT.class.isAssignableFrom(clazz);
    }


    public static String getName(Field field) {
        if (field.isAnnotationPresent(FieldName.class)) {
            return field.getAnnotationsByType(FieldName.class)[0].name();
        }
        return field.getName();
    }

    public static int getIndexOfField(Field field) {
        if (field.isAnnotationPresent(FieldIndex.class)) {
            return field.getAnnotationsByType(FieldIndex.class)[0].index();
        }
        // get index
        Field[] classFields = field.getDeclaringClass().getDeclaredFields();
        for (int i = 0; i < classFields.length; i++) {
            if (classFields[i].equals(field))
                return i;
        }
        return -1;
    }

}
