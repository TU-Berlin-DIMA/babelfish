package de.tub.dima.babelfish.storage.layout;

import com.oracle.truffle.api.*;
import de.tub.dima.babelfish.storage.layout.fields.*;
import de.tub.dima.babelfish.typesytem.record.*;
import de.tub.dima.babelfish.typesytem.schema.*;
import de.tub.dima.babelfish.typesytem.schema.field.*;
import de.tub.dima.babelfish.typesytem.udt.*;

import java.util.*;

public final class PhysicalSchema {

    @CompilerDirectives.CompilationFinal(dimensions = 1)
    private final PhysicalField[] fields;

    private final int fixedRecordSize;

    @CompilerDirectives.CompilationFinal(dimensions = 1)
    private final int[] fieldOffset;

    public PhysicalSchema(PhysicalField[] fields, boolean alignment) {
        this.fields = fields;
        this.fieldOffset = new int[fields.length];
        this.fixedRecordSize = calculateRecordSize(fields, alignment);
    }

    private int calculateRecordSize(PhysicalField[] fields, boolean alignment) {
        int recordSize = 0;
        for (int i = 0; i < fields.length; i++) {
            PhysicalField field = fields[i];
            int fieldSize = field.getPhysicalSize();
            if (alignment && recordSize % fieldSize != 0) {
                // add padding
                int paddedSize  = (recordSize + fieldSize - 1) & -fieldSize;
                System.out.println("PhysicalSchema: added padding for field " + field.getName() + " " + (paddedSize-recordSize));
                recordSize = paddedSize;
            }
            fieldOffset[i] = recordSize;

            recordSize += field.getPhysicalSize();
        }
        return recordSize;
    }


    public PhysicalField getField(long index) {
        return fields[(int) index];
    }

    public Int_8_PhysicalField getInt8Field(long index) {
        return (Int_8_PhysicalField) fields[(int) index];
    }

    public Int_16_PhysicalField getInt16Field(long index) {
        return (Int_16_PhysicalField) fields[(int) index];
    }

    public Int_32_PhysicalField getInt32Field(long index) {
        return (Int_32_PhysicalField) fields[(int) index];
    }

    public Int_64_PhysicalField getInt64Field(long index) {
        return (Int_64_PhysicalField) fields[(int) index];
    }

    public Fload_32_PhysicalField getFloat32Field(long index) {
        return (Fload_32_PhysicalField) fields[(int) index];
    }

    public Fload_64_PhysicalField getFloat64Field(long index) {
        return (Fload_64_PhysicalField) fields[(int) index];
    }


    public int getSize() {
        return fields.length;
    }

    public int getFixedRecordSize() {
        return fixedRecordSize;
    }

    public int getRecordOffset(long index) {
        return fieldOffset[(int) index];
    }

    public PhysicalField[] getFields() {
        return fields;
    }

    public static class Builder {
        private final List<PhysicalField> fields = new ArrayList<>();

        public Builder() {

        }

        public Builder(Schema schema) {
            for (SchemaField field : schema.getFields()) {
                addField(field);
            }
        }


        public Builder addField(SchemaField f) {
            Class field = f.getType();
            String name = f.getName();
            if (UDT.class.isAssignableFrom(field)) {
                try {
                    Class<? extends UDT> udt = (Class<? extends UDT>) field;
                    Schema udtSchema = RecordUtil.createSchemaFromUDT(udt);
                    PhysicalSchema physicalSchema = new Builder(udtSchema).build();
                    PhysicalField physicalField = new PhysicalUDTField(name, udt, physicalSchema);
                    addField(physicalField);
                } catch (SchemaExtractionException e) {
                    e.printStackTrace();
                }
            } else {
                PhysicalField physicalField = PhysicalFieldFactory.getPhysicalField(field, name, f);
                addField(physicalField);
            }
            return this;
        }

        public Builder addField(PhysicalField field) {
            fields.add(field);
            return this;
        }

        public PhysicalSchema build() {
            return build(false);
        }

        public PhysicalSchema build(boolean alignment) {
            PhysicalField[] array = fields.toArray(new PhysicalField[0]);
            return new PhysicalSchema(array, alignment);
        }
    }
}
