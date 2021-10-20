package de.tub.dima.babelfish.typesytem.schema.field;

import de.tub.dima.babelfish.typesytem.*;

public class VariableLengthField extends SchemaField {

    public VariableLengthField(String name, Class<? extends BFType> type, int index, boolean neverNull) {
        super(name, type, index, neverNull);
    }
}
