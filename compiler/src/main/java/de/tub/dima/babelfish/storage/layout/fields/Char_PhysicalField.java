package de.tub.dima.babelfish.storage.layout.fields;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;

public class Char_PhysicalField implements PhysicalField<Char> {

    private final String name;

    public Char_PhysicalField(String name) {
        this.name = name;
    }

    @Override
    public int getPhysicalSize() {
        return 2;
    }

    @Override
    public LuthStamps getStamp() {
        return LuthStamps.Char;
    }

    @Override
    public Char readValue(AddressPointer addressPointer) {
        char value = addressPointer.getChar();
        return new Char(value);
    }

    @Override
    public void writeValue(AddressPointer bufferAddress, Char value) {
        bufferAddress.putChar(value.getChar());
    }

    @Override
    public String getName() {
        return name;
    }

}
