package de.tub.dima.babelfish.typesytem.schema.field;

import de.tub.dima.babelfish.typesytem.*;

import java.io.*;
import java.lang.reflect.*;

public abstract class SchemaField implements Serializable {
    private final String name;
    private final Class<? extends BFType> type;
    private final int index;
    private final boolean neverNull;
    private transient final Field referenceField;

    public SchemaField(String name, Class<? extends BFType> type, int index, boolean neverNull, Field referenceField) {
        this.name = name;
        this.type = type;
        this.index = index;
        this.neverNull = neverNull;
        this.referenceField = referenceField;
    }

    public SchemaField(String name, Class<? extends BFType> type, int index, boolean neverNull) {
       this(name, type, index, neverNull, null);
    }

    public Class<? extends BFType> getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    public Field getReferenceField() {
        return referenceField;
    }

    @Override
    public String toString() {
        return "#" + index + "," + name + ":" + type.getSimpleName();
    }
}
