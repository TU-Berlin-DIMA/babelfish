package de.tub.dima.babelfish.storage.layout.fields;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;

public class Int_8_PhysicalField implements PhysicalField<Int_8> {

    private final String name;

    public Int_8_PhysicalField(String name) {
        this.name = name;
    }

    @Override
    public int getPhysicalSize() {
        return 1;
    }

    @Override
    public LuthStamps getStamp() {
        return LuthStamps.Int8;
    }

    @Override
    public Int_8 readValue(AddressPointer addressPointer) {
        byte value = addressPointer.getByte();
        return new Eager_Int_8(value);
    }

    @Override
    public void writeValue(AddressPointer bufferAddress, Int_8 value) {
        bufferAddress.putByte(value.asByte());
    }

    @Override
    public String getName() {
        return name;
    }

}
