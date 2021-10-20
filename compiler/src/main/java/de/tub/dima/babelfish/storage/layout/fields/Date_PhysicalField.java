package de.tub.dima.babelfish.storage.layout.fields;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.udt.LazyDate;
import de.tub.dima.babelfish.typesytem.udt.AbstractDate;

public class Date_PhysicalField implements PhysicalField<AbstractDate> {

    private final String name;


    public Date_PhysicalField(String name) {
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
    public AbstractDate readValue(AddressPointer addressPointer) {
        if(LAZY_READS) {
            return new LazyDate(addressPointer.getAddress());
        }else{
            int value = addressPointer.getInt();
            return new Date(value);
        }
    }

    @Override
    public void writeValue(AddressPointer bufferAddress, AbstractDate value) {
        bufferAddress.putInt(value.getUnixTs());
    }

    @Override
    public String getName() {
        return name;
    }

}
