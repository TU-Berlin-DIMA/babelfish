package de.tub.dima.babelfish.storage.layout.fields;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;

public class Numeric_PhysicalField implements PhysicalField<Numeric> {

    private final String name;

    private final int precision;

    public Numeric_PhysicalField(String name, int precision) {
        this.name = name;
        this.precision = precision;
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
    public Numeric readValue(AddressPointer addressPointer) {
        long value = addressPointer.getLong();
        return new EagerNumeric(value, precision);
    }

    @Override
    public void writeValue(AddressPointer bufferAddress, Numeric value) {
        bufferAddress.putLong(value.getValue());
    }

    @Override
    public String getName() {
        return name;
    }

}
