package de.tub.dima.babelfish.storage.layout.fields;

import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.schema.field.ArrayField;
import de.tub.dima.babelfish.typesytem.schema.field.NumericField;
import de.tub.dima.babelfish.typesytem.schema.field.SchemaField;
import de.tub.dima.babelfish.typesytem.schema.field.TextField;
import de.tub.dima.babelfish.typesytem.udt.AbstractDate;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.Bool;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.BFArray;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class PhysicalFieldFactory {

    public static PhysicalField getPhysicalField(Class type,
                                                 String name) {
        return getPhysicalField(type, name, null);
    }

    public static PhysicalField getPhysicalField(BFType type, String name) {
        if (type instanceof Int_8) {
            return new Int_8_PhysicalField(name);
        } else if (type instanceof Int_16) {
            return new Int_16_PhysicalField(name);
        } else if (type instanceof Int_32) {
            return new Int_32_PhysicalField(name);
        } else if (type instanceof Lazy_Int_32) {
            return new Int_32_PhysicalField(name);
        } else if (type instanceof Int_64) {
            return new Int_64_PhysicalField(name);
        } else if (type instanceof Float_32) {
            return new Fload_32_PhysicalField(name);
        } else if (type instanceof Float_64) {
            return new Fload_64_PhysicalField(name);
        } else if (type instanceof Bool) {
            return new Bool_PhysicalField(name);
        } else if (type instanceof Char) {
            return new Char_PhysicalField(name);
        } else if (type instanceof AbstractDate) {
            return new Date_PhysicalField(name);
        } else if (Text.class.isAssignableFrom(type.getClass())) {
            long maxLength = ((Text) type).length();
            return new TextFixed_PhysicalField(name, (int) maxLength);
        } else if (Numeric.class.isAssignableFrom(type.getClass())) {
            long precission = ((Numeric) type).getPrecision();
            return new Numeric_PhysicalField(name, (int) precission);
        }
        /*else if (type instanceof Date) {
            try {
                Schema udtSchema = RecordUtil.createSchemaFromUDT(Date.class);
                PhysicalSchema physicalSchema = new PhysicalSchema.Builder(udtSchema).build();
                return new PhysicalUDTField(name, Date.class, physicalSchema);
            } catch (SchemaExtractionException e) {
                e.printStackTrace();
            }
        }*/

        throw new RuntimeException("not implemented");
    }


    public static PhysicalField getPhysicalField(Class type,
                                                 String name,
                                                 SchemaField schemaField) {
        if (Int_8.class.isAssignableFrom(type)) {
            return new Int_8_PhysicalField(name);
        } else if (Int_16.class.isAssignableFrom(type)) {
            return new Int_16_PhysicalField(name);
        } else if (Int_32.class.isAssignableFrom(type)) {
            return new Int_32_PhysicalField(name);
        } else if (Int_64.class.isAssignableFrom(type)) {
            return new Int_64_PhysicalField(name);
        } else if (Float_32.class.isAssignableFrom(type)) {
            return new Fload_32_PhysicalField(name);
        } else if (Float_64.class.isAssignableFrom(type)) {
            return new Fload_64_PhysicalField(name);
        } else if (Bool.class.isAssignableFrom(type)) {
            return new Bool_PhysicalField(name);
        } else if (Char.class.isAssignableFrom(type)) {
            return new Char_PhysicalField(name);
        } else if (AbstractDate.class.isAssignableFrom(type)) {
            return new Date_PhysicalField(name);
        } else if (Text.class.isAssignableFrom(type)) {
            long maxLength = ((TextField) schemaField).getMaxLength();
            return new TextFixed_PhysicalField(name, (int) maxLength);
        }
        /*else if (type == Date.class) {
            try {
                Schema udtSchema = RecordUtil.createSchemaFromUDT(Date.class);
                PhysicalSchema physicalSchema = new PhysicalSchema.Builder(udtSchema).build();
                return new PhysicalUDTField(name, Date.class, physicalSchema);
            } catch (SchemaExtractionException e) {
                e.printStackTrace();
            }
            */

        else if (Numeric.class.isAssignableFrom(type)) {
            long precission = ((NumericField) schemaField).getPrecission();
            return new Numeric_PhysicalField(name, (int) precission);
        } else if (BFArray.class.isAssignableFrom(type)) {
            long dimensions = ((ArrayField) schemaField).getDimensions();
            long maxLength = ((ArrayField) schemaField).getMaxLength();
            Class<? extends BFType> component = ((ArrayField) schemaField).getComponentType();
            if (Int_32.class.isAssignableFrom(component)) {
                return new ArrayFixedInt32_PhysicalField(name, (int) dimensions, maxLength);
            } else if (Int_64.class.isAssignableFrom(component)) {
                return new ArrayFixedInt64_PhysicalField(name, (int) dimensions, maxLength);
            } else if (Float_32.class.isAssignableFrom(component)) {
                return new ArrayFixedFloat32_PhysicalField(name, (int) dimensions, maxLength);
            } else if (Float_64.class.isAssignableFrom(component)) {
                return new ArrayFixedFloat64_PhysicalField(name, (int) dimensions, maxLength);
            }
        }

        throw new RuntimeException("not implemented");
    }

    public static PhysicalField getPhysicalField(Class type, String name, int maxLength, int precission) {
        if (type == Int_8.class) {
            return new Int_8_PhysicalField(name);
        } else if (type == Int_16.class) {
            return new Int_16_PhysicalField(name);
        } else if (type == Int_32.class) {
            return new Int_32_PhysicalField(name);
        } else if (type == Int_64.class) {
            return new Int_64_PhysicalField(name);
        } else if (type == Float_32.class) {
            return new Fload_32_PhysicalField(name);
        } else if (type == Float_64.class) {
            return new Fload_64_PhysicalField(name);
        } else if (type == Bool.class) {
            return new Bool_PhysicalField(name);
        } else if (type == Char.class) {
            return new Char_PhysicalField(name);
        } else if (Text.class.isAssignableFrom(type)) {
            return new TextFixed_PhysicalField(name, (int) maxLength);
        } else if (Date.class.isAssignableFrom(type)) {
            return new Date_PhysicalField(name);
        }
        /*else if (type == Date.class) {
            try {
                Schema udtSchema = RecordUtil.createSchemaFromUDT(Date.class);
                PhysicalSchema physicalSchema = new PhysicalSchema.Builder(udtSchema).build();
                return new PhysicalUDTField(name, Date.class, physicalSchema);
            } catch (SchemaExtractionException e) {
                e.printStackTrace();
            }
        }*/
        else if (Numeric.class.isAssignableFrom(type)) {

            return new Numeric_PhysicalField(name, (int) precission);
        }

        throw new RuntimeException("not implemented");
    }

}
