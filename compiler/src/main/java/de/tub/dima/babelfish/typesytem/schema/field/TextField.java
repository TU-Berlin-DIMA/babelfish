package de.tub.dima.babelfish.typesytem.schema.field;

import de.tub.dima.babelfish.typesytem.BFType;

import java.lang.reflect.Field;

public class TextField extends SchemaField {

    private long maxLength = -1;

    public TextField(String name, Class<? extends BFType> type, int index, boolean neverNull, Field field, long maxLength) {
        super(name, type, index, neverNull, field);
        this.maxLength = maxLength;
    }

    public TextField(String name, Class<? extends BFType> type, int index, boolean neverNull, Field field) {
        super(name, type, index, neverNull, field);
    }

    public long getMaxLength() {
        return maxLength;
    }
}
