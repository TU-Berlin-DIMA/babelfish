package de.tub.dima.babelfish.storage.layout;

import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.layout.fields.LuthStamps;
import de.tub.dima.babelfish.typesytem.*;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;

public interface PhysicalField<T extends BFType> {
    final boolean LAZY_READS = RuntimeConfiguration.LAZY_READS;
    int getPhysicalSize();
    LuthStamps getStamp();

    T readValue(AddressPointer addressPointer);

    void writeValue(AddressPointer bufferAddress, T value);

    String getName();
}
