package de.tub.dima.babelfish.typesytem.udt;

import org.apache.arrow.memory.ArrowBuf;

public class ArrowSourceDate extends AbstractDate{

    private final ArrowBuf dataBuffer;
    private final long offset;

    public ArrowSourceDate(ArrowBuf dataBuffer, long offset) {
        this.dataBuffer = dataBuffer;
        this.offset = offset;
    }

    @Override
    public int getUnixTs() {
        return dataBuffer.getInt(offset);
    }
}
