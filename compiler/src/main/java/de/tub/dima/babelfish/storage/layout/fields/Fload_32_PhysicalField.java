package de.tub.dima.babelfish.storage.layout.fields;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.storage.layout.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.*;

public class Fload_32_PhysicalField implements PhysicalField<Float_32> {

    private final String name;

    public Fload_32_PhysicalField(String name) {
        this.name = name;
    }


    @Override
    public int getPhysicalSize() {
        return 4;
    }

    @Override
    public LuthStamps getStamp() {
        return LuthStamps.Float32;
    }


    @Override
    public Float_32 readValue(AddressPointer addressPointer) {
        return new Eager_Float_32(addressPointer.getFloat());
    }

    @Override
    public void writeValue(AddressPointer bufferAddress, Float_32 value) {
        bufferAddress.putFloat(value.asFloat());
    }

    public final void add(AddressPointer bufferAddress, Float_32 value){
        int expectedBits;
        float input = value.asFloat();
        float v;
        do {
            // Load and CAS with the raw bits to avoid issues with NaNs and
            // possible bit conversion from signaling NaNs to quiet NaNs that
            // may result in the loop not terminating.
            expectedBits = bufferAddress.getIntVolatile();
            v = Float.intBitsToFloat(expectedBits);
        } while (!UnsafeUtils.weakCompareAndSetInt(bufferAddress.getAddress(),
                expectedBits, Float.floatToRawIntBits(v + input)));
    }

    @Override
    public String getName() {
        return name;
    }

}
