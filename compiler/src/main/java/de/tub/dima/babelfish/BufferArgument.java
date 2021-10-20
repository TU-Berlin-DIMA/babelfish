package de.tub.dima.babelfish;

import com.oracle.truffle.api.interop.*;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;

public class BufferArgument implements TruffleObject {

   private final Buffer buffer;
    private final BufferManager buffrManager;

    public BufferArgument(Buffer buffer, BufferManager buffrManager) {
        this.buffer = buffer;
        this.buffrManager = buffrManager;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public BufferManager getBufferManager() {
        return buffrManager;
    }
}
