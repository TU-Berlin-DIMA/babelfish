package de.tub.dima.babelfish.typesytem.schema.field;

import de.tub.dima.babelfish.typesytem.udt.*;

import java.lang.reflect.*;

public class UDTField extends SchemaField {
    public UDTField(String name, Class<? extends UDT> type, int index, boolean neverNull, Field field) {
        super(name, type, index, neverNull, field);
    }
}
