package de.tub.dima.babelfish.storage.layout.fields;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.*;
import de.tub.dima.babelfish.typesytem.valueTypes.*;

public class Bool_PhysicalField implements PhysicalField<Bool> {

    private final String name;

    public Bool_PhysicalField(String name) {
        this.name = name;
    }

    @Override
    public int getPhysicalSize() {
        return 1;
    }

    @Override
    public LuthStamps getStamp() {
        return LuthStamps.Bool;
    }

    @Override
    public Bool readValue(AddressPointer addressPointer) {
        boolean value = addressPointer.getBoolean();
        return new Bool(value);
    }

    @Override
    public void writeValue(AddressPointer bufferAddress, Bool value) {
        bufferAddress.putBoolean(value.getValue());
    }

    @Override
    public String getName() {
        return name;
    }

}
