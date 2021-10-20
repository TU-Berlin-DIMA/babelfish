package de.tub.dima.babelfish.typesytem.schema.field;

import de.tub.dima.babelfish.typesytem.BFType;

import java.lang.reflect.Field;

public class NumericField extends SchemaField {


    private final int precission;

    public NumericField(String name, int index, Class<BFType> type, boolean notNull, Field referenceField, int precission) {
        super(name, type, index, notNull, referenceField);
        this.precission = precission;
    }

    public int getPrecission() {
        return precission;
    }
}
