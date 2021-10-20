package de.tub.dima.babelfish.ir.lqp.schema;

import de.tub.dima.babelfish.typesytem.BFType;

public class FieldConstant<T extends BFType> implements FieldStamp<T> {

    private final Class<T> type;
    private final Object value;

    public FieldConstant(T value) {
        this.type = (Class<T>) value.getClass();
        this.value = value;
    }

    @Override
    public String getName() {
        return value.toString();
    }

    public Object getValue() {
        return value;
    }
}
