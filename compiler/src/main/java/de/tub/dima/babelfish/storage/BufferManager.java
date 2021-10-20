package de.tub.dima.babelfish.storage;

import java.util.*;

/**
 * Central buffer manager.
 * Allocates all memory on demand.
 */
public class BufferManager {

    Set<Buffer> usedBuffers = new HashSet<>();
    Map<Unit.Bytes, Deque<Buffer>> freeBuffers = new HashMap<>();

    public synchronized Buffer allocateBuffer(Unit.Bytes size){
        AddressPointer startAddress = UnsafeUtils.allocateMemory(size);

        if(freeBuffers.containsKey(size) && !freeBuffers.get(size).isEmpty())
            return freeBuffers.get(size).pollFirst();
        Buffer newBuffer = new Buffer(startAddress, size);
        usedBuffers.add(newBuffer);
        return newBuffer;
    }

    public synchronized void release(Buffer buffer){
        Unit.Bytes size = buffer.getAllocatedSize();
        freeBuffers.putIfAbsent(size, new LinkedList<>());
        freeBuffers.get(size).addFirst(buffer);
    }

    public void releaseAll() {
        for(Buffer b: usedBuffers){
            Unit.Bytes size = b.getAllocatedSize();
            freeBuffers.putIfAbsent(size, new LinkedList<>());
            freeBuffers.get(size).addFirst(b);
        }

        usedBuffers.clear();
    }
}
