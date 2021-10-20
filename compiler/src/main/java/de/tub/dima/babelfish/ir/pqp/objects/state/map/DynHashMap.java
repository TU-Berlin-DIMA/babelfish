package de.tub.dima.babelfish.ir.pqp.objects.state.map;

import com.oracle.truffle.api.CompilerDirectives;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.ir.pqp.nodes.state.map.HashMapFindEntryNode;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateVariable;

/**
 * HashMap implementations following Kersten et al. VLDB Vol. 11, No. 13
 * "Everything you always wanted to know about compiled and vectorized queries but were afraid to ask"
 * https://github.com/TimoKersten/db-engine-paradigms/blob/master/include/common/runtime/Hashmap.hpp
 *
 * This object should be acced via. primitive ir nodes. e.g., {@link HashMapFindEntryNode} *
 *
 */
public class DynHashMap implements BFStateVariable {

    private static final long POINTER_SIZE = 8;
    private static final long ENTRY_HEADER_SIZE = 16;
    private static final long CACHE_LINE_SIZE = 64;


    private final long maxEntryOffset = 0;
    private final long entryBufferOffset;
    private final long keyBufferOffset;
    private final long bufferAddress;
    private final long capacity;
    private final int valueSize;
    private final long entrySize;
    private final long mask;
    private final long rawBufferAddress;
    private final long nrEntries;


    public int getValueSize() {
        return valueSize;
    }

    public long getCapacity() {
        return capacity;
    }

    public static class EntryHandler {
        /**
         * Offset to the pointer of the next entry
         */
        private final static long NEXT_POINTER_OFFSET = 0;

        /**
         * Offset to the hash value of this entry
         */
        private final static long HASH_OFFSET = 8;

        /**
         * Offset to the value of this entry
         */
        private final static long VALUE_OFFSET = 16;

        public static long getNext(long address) {
            return UnsafeUtils.getLong(address + NEXT_POINTER_OFFSET);
        }

        public static long getHash(long address) {
            return UnsafeUtils.getLong(address + HASH_OFFSET);
        }

        public static void setHash(long address, long hash) {
            UnsafeUtils.putLong(address + HASH_OFFSET, hash);
        }

        public static void setNext(long address, long next) {
            UnsafeUtils.putLong(address + NEXT_POINTER_OFFSET, next);
        }

        public static long getValueAddress(long address) {
            return address + VALUE_OFFSET;
        }

        public static long getLockAddress(long address) {
            return address + HASH_OFFSET;
        }

        public static boolean isNull(long address) {
            return address == 0;
        }

        public static void setLongValue(long address, long value) {
            UnsafeUtils.putLong(getValueAddress(address), value);
            //System.out.println("write to " + getValueAddress() + " : " + value);
        }

        public static long getLongValue(long address) {
            //System.out.println("read from " + getValueAddress());
            return UnsafeUtils.getLong(getValueAddress(address));
        }

    }

    public DynHashMap(long nrEntries, int valueSize) {
        this.nrEntries = nrEntries;
        double loadFactor = 0.7;
        long exp = 64 - Long.numberOfLeadingZeros(nrEntries);
        //assert(exp < sizeof(hash_t) * 8);
        if (((long) 1 << exp) < nrEntries / loadFactor) exp++;
        this.capacity = ((long) 1) << exp;
        this.mask = capacity - 1;

        this.valueSize = valueSize;

        /**
         * memory layout
         * 8 Byte max entry value + padding cache line
         * key space (capacity * pointer_size)bytes + padding cache line
         * entry space (capacity * entrySize)bytes
         */
        this.keyBufferOffset = CACHE_LINE_SIZE;
        // calculate the key space size
        long keySpaceSize = capacity * POINTER_SIZE;

        // add padding for key space as entry offset
        this.entryBufferOffset = this.keyBufferOffset +
                (keySpaceSize + CACHE_LINE_SIZE -1) & -CACHE_LINE_SIZE;

        // calculate entry size
        long alignedValueSize = (valueSize + POINTER_SIZE -1) & -POINTER_SIZE;
        this.entrySize = alignedValueSize + ENTRY_HEADER_SIZE;
        long entryBufferSize = capacity * entrySize;
        long totalBufferSize = entryBufferOffset + entryBufferSize + CACHE_LINE_SIZE;
        this.rawBufferAddress = UnsafeUtils.UNSAFE.allocateMemory(totalBufferSize);
        this.bufferAddress = (rawBufferAddress + CACHE_LINE_SIZE -1) & -CACHE_LINE_SIZE;
        for (int i = 0; i < totalBufferSize; i++) {
            UnsafeUtils.putByte(bufferAddress + i, (byte) 0);
        }
    }


    public long addEntry(long hash) {
        long nextEntryAddress = getNextEntry();
        long oldAddress = insert(hash, nextEntryAddress);
        EntryHandler.setHash(nextEntryAddress, hash);
        //EntryHandler.setKey(nextEntryAddress, key);
        EntryHandler.setNext(nextEntryAddress, oldAddress);
        return nextEntryAddress;
    }


    public void free() {
        UnsafeUtils.UNSAFE.freeMemory(rawBufferAddress);
    }

    public long getNextEntry() {

        if (CompilerDirectives.inInterpreter()) {
            if (getMaxEntry() == capacity) {
                print();
                throw new RuntimeException("Ups hashmap is full");
            }
        }
        long nextBufferAddress = getEntryBuffer() + (entrySize * getMaxEntry());
        incrementMaxEntry();
        return nextBufferAddress;
    }

    private void incrementMaxEntry(){
        long oldMaxEntry = getMaxEntry();
        UnsafeUtils.UNSAFE.putLong(bufferAddress + maxEntryOffset, oldMaxEntry +1);
    }


    public long insert(long hash, long address) {
        long pos = hash & mask;
        long keyAddress = getKeyBuffer() + (pos * 8);
        long old = UnsafeUtils.getLong(keyAddress);
        UnsafeUtils.putLong(keyAddress, address);
        return old;
    }


    public long hasEntry(long key) {
        long hash = hashKey(key);
        long entry = findEntry(key, hash);
        return entry;
    }

    public long getEntrySize() {
        return entrySize;
    }

    public long findEntry(long key, long hash) {
        long pos = hash & mask;
        long entry = getEntry(pos);
        if (entry == 0)
            return 0;

        for (; entry != 0; entry = EntryHandler.getNext(entry)) {
            //if (EntryHandler.getKey(entry) == key) return entry;
        }
        // while (!EntryHandler.isNull(entry) && EntryHandler.getKey(entry) != key) {
        //     entry = EntryHandler.getNext(entry);
        //}
        return 0;
    }


    public long getEntry(long position) {
        long entryAddress = UnsafeUtils.UNSAFE.getLong(getKeyAddress(position));
        return entryAddress;
    }

    public long getKeyAddress(long position) {
        return bufferAddress + keyBufferOffset + (position * 8);
    }

    long hashKey(long k) {
        //byte[] hash = crc32.int2quad(k);
        //int h = crc32.byte2crc32sum(0x04c11db7, hash);
        //return ((h << 32) | h) * 0x2545F4914F6CDD1DL;
        return (hashKey(k, 902850234));
    }

    long hashKey(long k, long seed) {
        // MurmurHash64A
        long m = 0xc6a4a7935bd1e995L;
        int r = 47;
        long h = seed ^ 0x8445d61a4e774912L ^ (8 * m);
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        h ^= h >> r;
        h *= m;
        h ^= h >> r;
        return h;
    }


    @CompilerDirectives.TruffleBoundary
    public void dump() {

        java.util.HashMap<Long, Object> map = new java.util.HashMap<Long, Object>();
        long sum = 0;
        for (int i = 0; i < capacity; i++) {
            long entry = getEntry(i);
            long counter = 0;
            for (; entry != 0; entry = EntryHandler.getNext(entry)) {
                counter++;
            }

            sum = sum + counter;
            System.out.print(counter + ", ");
        }

        System.out.println("average/depth:" + (sum / (double) capacity));
        System.out.println(map);
    }

    @CompilerDirectives.TruffleBoundary
    public static void print(long buffer, long maxEntry, long entrySize) {

        java.util.HashMap<Long, Object> map = new java.util.HashMap<Long, Object>();
        for (int i = 0; i < maxEntry; i++) {
            long e = (buffer + i * entrySize);
            System.out.print(UnsafeUtils.getInt(DynHashMap.EntryHandler.getValueAddress(e)));
        }
        System.out.println(map);
    }

    public void print() {
        print(getEntryBuffer(), getMaxEntry(), entrySize);
    }

    @Override
    public String toString() {

        java.util.HashMap<Integer, Integer> map = new java.util.HashMap<Integer, Integer>();
        for (int i = 0; i < getMaxEntry(); i++) {
            long e = (getEntryBuffer() + i * entrySize);
            map.put(i, UnsafeUtils.getInt(DynHashMap.EntryHandler.getValueAddress(e)));
        }
        return map.toString();
    }

    public long getMaxEntry() {
        return UnsafeUtils.getLong(bufferAddress + maxEntryOffset);
    }

    public long getEntryBuffer() {
        return bufferAddress + entryBufferOffset;
    }

    public long getMask() {
        return mask;
    }

    public long getKeyBuffer() {
        return bufferAddress + keyBufferOffset;
    }

    public long getKeyBufferOffset() {
        return keyBufferOffset;
    }

    public long getEntryBufferOffset() {
        return entryBufferOffset;
    }

    public long getBufferAddress() {
        return bufferAddress;
    }

    public long getNrEntries() {
        return nrEntries;
    }
}
