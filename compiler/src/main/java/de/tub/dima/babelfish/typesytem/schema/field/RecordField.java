package de.tub.dima.babelfish.typesytem.schema.field;

import de.tub.dima.babelfish.typesytem.record.*;

public class RecordField extends SchemaField {

    public RecordField(String name, Class<? extends Record> type, int index, boolean neverNull) {
        super(name, type, index, neverNull);
    }
}
