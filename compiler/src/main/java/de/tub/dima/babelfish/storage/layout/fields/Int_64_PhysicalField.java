package de.tub.dima.babelfish.storage.layout.fields;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;

public class Int_64_PhysicalField implements PhysicalField<Int_64> {

    private final String name;

    public Int_64_PhysicalField(String name) {
        this.name = name;
    }

    @Override
    public int getPhysicalSize() {
        return 8;
    }

    @Override
    public LuthStamps getStamp() {
        return LuthStamps.Int64;
    }
    @Override
    public Int_64 readValue(AddressPointer addressPointer) {
        long value = addressPointer.getLong();
        return new Eager_Int_64(value);
    }

    @Override
    public void writeValue(AddressPointer bufferAddress, Int_64 value) {
        bufferAddress.putLong(value.asLong());
    }

    @Override
    public String getName() {
        return name;
    }

}
