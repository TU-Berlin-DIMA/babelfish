package de.tub.dima.babelfish.storage.layout.fields;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.BFArray;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.PointerDoubleBFArray;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.PointerIntBFArray;

public class ArrayFixedFloat64_PhysicalField implements PhysicalField<BFArray> {

    private final String name;
    private final long maxLength;

    public ArrayFixedFloat64_PhysicalField(String name, int dimensions, long maxLength) {
        this.name = name;
        this.maxLength = maxLength;
    }

    @Override
    public int getPhysicalSize() {
        return 8 * (int) maxLength;
    }

    @Override
    public LuthStamps getStamp() {
        return LuthStamps.Float64;
    }

    @Override
    public PointerDoubleBFArray readValue(AddressPointer addressPointer) {
        return new PointerDoubleBFArray(addressPointer.getAddress(), (int) maxLength);
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
