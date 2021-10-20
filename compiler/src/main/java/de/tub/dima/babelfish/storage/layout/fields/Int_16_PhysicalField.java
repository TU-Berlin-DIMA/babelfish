package de.tub.dima.babelfish.storage.layout.fields;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;

public class Int_16_PhysicalField implements PhysicalField<Int_16> {

    private final String name;

    public Int_16_PhysicalField(String name) {
        this.name = name;
    }

    @Override
    public int getPhysicalSize() {
        return 2;
    }

    @Override
    public LuthStamps getStamp() {
        return LuthStamps.Int16;
    }

    @Override
    public Int_16 readValue(AddressPointer addressPointer) {
        short value = addressPointer.getShort();
        return new Eager_Int_16(value);
    }

    @Override
    public void writeValue(AddressPointer bufferAddress, Int_16 value) {
        bufferAddress.putShort(value.asShort());
    }

    @Override
    public String getName() {
        return name;
    }

}
