package de.tub.dima.babelfish.storage.layout.fields;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;

public class Int_32_PhysicalField implements PhysicalField<Int_32> {

    private final String name;

    public Int_32_PhysicalField(String name) {
        this.name = name;
    }

    @Override
    public int getPhysicalSize() {
        return 4;
    }

    @Override
    public LuthStamps getStamp() {
        return LuthStamps.Int32;
    }
    @Override
    public Int_32 readValue(AddressPointer addressPointer) {
        int value = addressPointer.getInt();
        return new Eager_Int_32(value);
        //return new Lazy_Int_32(addressPointer.getAddress());
    }

    @Override
    public void writeValue(AddressPointer bufferAddress, Int_32 value) {
        bufferAddress.putInt(value.asInt());
    }

    @Override
    public String getName() {
        return name;
    }

}
