package de.tub.dima.babelfish.typesytem.valueTypes.number.numeric;

import org.apache.arrow.memory.ArrowBuf;

public class ArrowSourceNumeric extends Numeric {

    private final ArrowBuf dataBuffer;
    private final long offset;

    public ArrowSourceNumeric(ArrowBuf dataBuffer, long offset, int precision) {
        super(precision);
        this.dataBuffer = dataBuffer;
        this.offset = offset;
    }

    @Override
    public long getValue() {
        return dataBuffer.getInt(offset);
    }
}
