package de.tub.dima.babelfish.storage.layout.fields;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.BFArray;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.PointerFloatBFArray;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.PointerIntBFArray;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.PointerLongBFArray;

public class ArrayFixedInt64_PhysicalField implements PhysicalField<BFArray> {

    private final String name;
    private final long maxLength;

    public ArrayFixedInt64_PhysicalField(String name, int dimensions, long maxLength) {
        this.name = name;
        this.maxLength = maxLength;
    }

    @Override
    public int getPhysicalSize() {
        return 8 * (int) maxLength;
    }

    @Override
    public LuthStamps getStamp() {
        return LuthStamps.Float32;
    }

    @Override
    public PointerLongBFArray readValue(AddressPointer addressPointer) {
        return new PointerLongBFArray(addressPointer.getAddress(), (int) maxLength);
    }

    @Override
    public void writeValue(AddressPointer bufferAddress, BFArray value) {
        PointerIntBFArray sourceArray = (PointerIntBFArray) value;
        PointerIntBFArray destArray = new PointerIntBFArray(bufferAddress.getAddress(), (int) maxLength);
        //for (int i = 0; i < maxLength; i++) {
            //destArray.write(i, sourceArray.read(i));

        //}
    }

    @Override
    public String getName() {
        return name;
    }

}
