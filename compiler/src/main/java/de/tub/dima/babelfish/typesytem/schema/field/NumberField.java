package de.tub.dima.babelfish.typesytem.schema.field;

import de.tub.dima.babelfish.typesytem.valueTypes.number.*;

import java.lang.reflect.*;

public class NumberField extends SchemaField {

    private Number min;
    private Number max;

    public NumberField(String name, int index, Class<NumberType> type, boolean notNull, Number min, Number max) {
        super(name, type, index, notNull);
        this.min = min;
        this.max = max;
    }

    public NumberField(String name, int index, Class<NumberType> type, boolean notNull, Number min, Number max, Field referenceField) {
        super(name, type, index, notNull, referenceField);
        this.min = min;
        this.max = max;
    }

}
