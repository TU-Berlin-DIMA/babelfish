package de.tub.dima.babelfish.typesytem.schema.field;

import de.tub.dima.babelfish.typesytem.BFType;

import java.lang.reflect.Field;

public class DateField extends SchemaField {

    public DateField(String name, int index, Class<BFType> type, boolean notNull, Field referenceField) {
        super(name, type, index, notNull, referenceField);
    }

}
