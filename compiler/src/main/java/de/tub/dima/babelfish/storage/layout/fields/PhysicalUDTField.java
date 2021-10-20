package de.tub.dima.babelfish.storage.layout.fields;

import com.oracle.truffle.api.*;
import com.oracle.truffle.api.nodes.*;
import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.storage.layout.*;
import de.tub.dima.babelfish.typesytem.*;
import de.tub.dima.babelfish.typesytem.udt.*;

import java.lang.reflect.*;

public class PhysicalUDTField<T extends UDT> implements PhysicalField<T> {

    private final Class<T> udt;
    private final String name;
    private final PhysicalSchema physicalSchema;
    @CompilerDirectives.CompilationFinal(dimensions = 1)
    private final Field[] udtFields;

    public PhysicalUDTField(String name, Class<T> udt, PhysicalSchema physicalSchema) {
        this.udt = udt;
        this.name = name;
        this.physicalSchema = physicalSchema;
        this.udtFields = udt.getDeclaredFields();
    }

    @Override
    public int getPhysicalSize() {
        return this.physicalSchema.getFixedRecordSize();
    }

    @Override
    public LuthStamps getStamp() {
        return LuthStamps.Bool;
    }

    @Override
    public T readValue(AddressPointer addressPointer) {
        try {
            T instance = UnsafeUtils.allocateInstance(udt);
            setFields(instance, addressPointer);
            return instance;
        } catch (InstantiationException e) {
           // e.printStackTrace();
        }
        return null;
    }

    @ExplodeLoop
    private void setFields(T object, AddressPointer startAddress) {
        long currentAddress = startAddress.getAddress();
        for (int i = 0; i<udtFields.length;i++) {
            PhysicalField physicalField = physicalSchema.getField(i);
            BFType value = physicalField.readValue(AddressPointer.box(currentAddress));
            GenericSerializer.setFieldToObject(object, udtFields[i], value);
            currentAddress += physicalField.getPhysicalSize();
        }
    }

    @Override
    @ExplodeLoop
    public void writeValue(AddressPointer bufferAddress, T object) {
        long currentAddress = bufferAddress.getAddress();
        for (int i = 0; i<udtFields.length;i++) {
            PhysicalField physicalField = physicalSchema.getField(i);
            BFType value = GenericSerializer.getValueFromObject(object, udtFields[i]);
            physicalField.writeValue(AddressPointer.box(currentAddress), value);
            currentAddress += physicalField.getPhysicalSize();
        }
    }

    @Override
    public String getName() {
        return name;
    }
}
