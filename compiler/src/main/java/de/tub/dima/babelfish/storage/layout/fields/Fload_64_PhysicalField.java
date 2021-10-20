package de.tub.dima.babelfish.storage.layout.fields;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.*;

public class Fload_64_PhysicalField implements PhysicalField<Float_64> {

    private final String name;

    public Fload_64_PhysicalField(String name) {
        this.name = name;
    }


    @Override
    public int getPhysicalSize() {
        return 4;
    }

    @Override
    public LuthStamps getStamp() {
        return LuthStamps.Float64;
    }

    @Override
    public Float_64 readValue(AddressPointer addressPointer) {
        return new Eager_Float_64(addressPointer.getDouble());
    }

    @Override
    public void writeValue(AddressPointer bufferAddress, Float_64 value) {
        bufferAddress.putDouble(value.asDouble());
    }

    @Override
    public String getName() {
        return name;
    }
}
