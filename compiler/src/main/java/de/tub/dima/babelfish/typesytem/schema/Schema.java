package de.tub.dima.babelfish.typesytem.schema;

import com.oracle.truffle.api.CompilerDirectives;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.schema.field.*;

import java.io.*;
import java.util.*;

public class Schema implements Serializable {

    private final String name;
    @CompilerDirectives.CompilationFinal(dimensions = 1)
    private final SchemaField[] fields;

    @CompilerDirectives.CompilationFinal()
    private final Class<? extends Record> recordClass;

    Schema(String name, List<SchemaField> fields, Class<? extends Record> recordClass) {
        this.name = name;
        this.fields = fields.toArray(new SchemaField[0]);
        this.recordClass = recordClass;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (SchemaField f : this.fields) {
            sb.append("(").append(f.toString()).append(")");
            sb.append(",");
        }
        return sb.toString();
    }

    public SchemaField[] getFields() {
        return fields;
    }

    public Class<? extends Record> getRecordClass() {
        return recordClass;
    }
}
