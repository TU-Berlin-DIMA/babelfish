package de.tub.dima.babelfish.typesytem.schema.field;

import de.tub.dima.babelfish.typesytem.*;

import java.lang.reflect.*;

public class ValueField extends SchemaField {

    public ValueField(String name, Class<? extends BFType> type, int index, boolean neverNull) {
        super(name, type, index, neverNull);
    }

    public ValueField(String name, Class<? extends BFType> type, int index, boolean neverNull, Field referenceField) {
        super(name, type, index, neverNull, referenceField);
    }
}
