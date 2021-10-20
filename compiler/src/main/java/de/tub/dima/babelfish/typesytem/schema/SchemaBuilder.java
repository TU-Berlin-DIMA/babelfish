package de.tub.dima.babelfish.typesytem.schema;

import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.schema.field.*;

import java.util.*;

public class SchemaBuilder {
    private final List<SchemaField> fields = new ArrayList<>();
    private final Class<? extends Record> recordClass;

    private SchemaBuilder(Class<? extends Record> recordClass) {
        this.recordClass = recordClass;
    }

    private SchemaBuilder() {
        this.recordClass = null;
    }

    public SchemaBuilder addField(SchemaField field) {
        fields.add(field);
        return this;
    }

    public Schema build() {
        return new Schema("", fields,recordClass);
    }

    public static SchemaBuilder createBuilder() {
        return new SchemaBuilder();
    }

    public static SchemaBuilder createBuilder(Class<? extends Record> recordClass) {
        return new SchemaBuilder(recordClass);
    }
}