package de.tub.dima.babelfish.typesytem.schema.field;

import de.tub.dima.babelfish.typesytem.*;
import de.tub.dima.babelfish.typesytem.record.*;

import java.lang.reflect.Field;

public class ArrayField extends SchemaField {

    private Class<? extends BFType> componentType;
    private final int dimensions;
    private final long maxLength;

    public ArrayField(Field field, String name,
                      Class<? extends BFType> type,
                      int index,
                      boolean neverNull,
                      Class<? extends BFType> componentType,
                      int dimensions, long maxLength) {
        super(name, type, index, neverNull, field);
        this.componentType = componentType;
        this.dimensions = dimensions;
        this.maxLength = maxLength;
    }

    public Class<? extends BFType> getComponentType() {
        return componentType;
    }

    public boolean isRecordComponent(){
        return RecordUtil.isLuthRecord(this.componentType);
    }

    public boolean isLuthUDT(){
        return RecordUtil.isLuthUDT(this.componentType);
    }

    @Override
    public String toString() {
        return "(#" + getIndex() + "," + getName() + ":" + "BFArray"+dimensions+"<"+componentType.getSimpleName()+">)";
    }

    public int getDimensions() {
        return dimensions;
    }

    public long getMaxLength() {
        return maxLength;
    }
}
