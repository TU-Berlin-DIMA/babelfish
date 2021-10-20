package de.tub.dima.babelfish.ir.lqp.schema;

import de.tub.dima.babelfish.typesytem.BFType;

public class FieldReference<T extends BFType> implements FieldStamp<T> {

    private final String key;
    private final Class<T> type;
    private final int maxLength;

    public FieldReference(String key, Class<T> type) {
        this.key = key;
        this.type = type;
        maxLength = 0;
    }

    public FieldReference(String key, Class<T> type, int maxLength) {
        this.key = key;
        this.type = type;
        this.maxLength = maxLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public String getKey() {
        return key;
    }

    public Class<T> getType() {
        return type;
    }

    @Override
    public String getName() {
        return key;
    }
}
