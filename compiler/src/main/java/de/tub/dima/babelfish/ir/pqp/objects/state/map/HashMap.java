package de.tub.dima.babelfish.ir.pqp.objects.state.map;

import com.oracle.truffle.api.CompilerDirectives;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateVariable;

/**
 * HashMap implementations following Kersten et al. VLDB Vol. 11, No. 13
 * "Everything you always wanted to know about compiled and vectorized queries but were afraid to ask"
 * https://github.com/TimoKersten/db-engine-paradigms/blob/master/include/common/runtime/Hashmap.hpp
 */
public class HashMap implements BFStateVariable {

    private static final long POINTER_OFFSET = 8;
    private static final long ENTRY_HEADER_SIZE = 24;

    private final long buffer;
    private final long capacity;
    private final int valueSize;
    private long maxEntry;
    private final long keys;
    private final long entrySize;
    private final long mask;

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
         * Offset to the key value of this entry
         */
        private final static long KEY_OFFSET = 16;

        /**
         * Offset to the value of this entry
         */
        private final static long VALUE_OFFSET = 24;

        public static long getNext(long address) {
            return UnsafeUtils.getLong(address + NEXT_POINTER_OFFSET);
        }

        public static long getHash(long address) {
            return UnsafeUtils.getLong(address + HASH_OFFSET);
        }

        public static void setHash(long address, long hash) {
            UnsafeUtils.putLong(address + HASH_OFFSET, hash);
        }

        public static long getKey(long address) {
            return UnsafeUtils.getLong(address + KEY_OFFSET);
        }

        public static void setKey(long address, long key) {
            UnsafeUtils.putLong(address + KEY_OFFSET, key);
        }

        public static void setNext(long address, long next) {
            UnsafeUtils.putLong(address + NEXT_POINTER_OFFSET, next);
        }

        public static long getValueAddress(long address) {
            return address + VALUE_OFFSET;
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

    public HashMap(long nrEntries, int valueSize) {
        this.entrySize = valueSize + ENTRY_HEADER_SIZE;


        double loadFactor = 0.7;
        long exp = 64 - Long.numberOfLeadingZeros(nrEntries);
        //assert(exp < sizeof(hash_t) * 8);
        if (((long) 1 << exp) < nrEntries / loadFactor) exp++;
        this.capacity = ((long) 1) << exp;
        this.mask = capacity - 1;

        this.valueSize = valueSize;
        buffer = UnsafeUtils.UNSAFE.allocateMemory(capacity * entrySize);
        keys = UnsafeUtils.UNSAFE.allocateMemory(capacity * 8);
        for (int i = 0; i < capacity * 8; i++) {
            UnsafeUtils.putLong(keys + i, (byte) 0);
        }
        for (int i = 0; i < capacity * entrySize; i++) {
            //UnsafeUtils.putByte(buffer + i, (byte) 0);
        }
    }

    public long addEntry(long key) {
        return addEntry(key, hashKey(key));
    }

    public long addEntry(long key, long hash) {
        long nextEntryAddress = getNextEntry();
        long oldAddress = insert(hash, nextEntryAddress);
        EntryHandler.setHash(nextEntryAddress, hash);
        EntryHandler.setKey(nextEntryAddress, key);
        EntryHandler.setNext(nextEntryAddress, oldAddress);
        return nextEntryAddress;
    }


    public void put(long key, long value) {
        long hash = hashKey(key);
        long entry = findEntry(key, hash);
        if (entry != 0) {
            //if (entry.getKey() != key) {
            //    throw new RuntimeException("Collision hash: " + hash + " " + entry.getKey() + " vs. " + key );
            //}
            EntryHandler.setLongValue(entry, value);
        } else {
            long newEntry = addEntry(key, hash);
            EntryHandler.setLongValue(newEntry, value);
        }
    }

    public long getNextEntry() {

        if (CompilerDirectives.inInterpreter()) {
            if (maxEntry == capacity)
                throw new RuntimeException("Ups hashmap is full capacity = " + capacity);
        }
        long nextBufferAddress = buffer + (entrySize * maxEntry);
        maxEntry++;
        return nextBufferAddress;
    }

    public long insert(long hash, long address) {
        long pos = hash & mask;
        long keyAddress = keys + (pos * 8);
        long old = UnsafeUtils.getLong(keyAddress);
        UnsafeUtils.putLong(keyAddress, address);
        return old;
    }

    public long findEntry(long key) {
        long hash = hashKey(key);
        long entry = findEntry(key, hash);
        if (entry != 0)
            return EntryHandler.getValueAddress(entry);
        return EntryHandler.getValueAddress(addEntry(key, hash));
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
            if (EntryHandler.getKey(entry) == key) return entry;
        }
        // while (!EntryHandler.isNull(entry) && EntryHandler.getKey(entry) != key) {
        //     entry = EntryHandler.getNext(entry);
        //}
        return 0;
    }

    public void free() {
        UnsafeUtils.UNSAFE.freeMemory(keys);
        UnsafeUtils.UNSAFE.freeMemory(buffer);
    }


    public long getEntry(long position) {
        long entryAddress = UnsafeUtils.UNSAFE.getLong(keys + (position * 8));
        return entryAddress;
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
            map.put(EntryHandler.getKey(e), UnsafeUtils.getFloat(e));
        }
        System.out.println(map);
    }

    public void print() {
        print(buffer, maxEntry, entrySize);
    }

    @Override
    public String toString() {

        java.util.HashMap<Long, Long> map = new java.util.HashMap<Long, Long>();
        for (int i = 0; i < maxEntry; i++) {
            long e = (buffer + i * entrySize);
            map.put(EntryHandler.getKey(e), EntryHandler.getLongValue(e));
        }
        return map.toString();
    }

    public long getMaxEntry() {
        return maxEntry;
    }

    public long getBuffer() {
        return buffer;
    }
}
