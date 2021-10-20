package de.tub.dima.babelfish.storage;


import sun.misc.*;
//import jdk.internal.misc.*;

import java.lang.reflect.*;

/**
 * Utility methods to work with raw memory.
 */
public class UnsafeUtils {

    public static final Unsafe UNSAFE = initUnsafe();

    private static Unsafe initUnsafe() {
        try {
            // Fast path when we are trusted.
            return Unsafe.getUnsafe();
        } catch (SecurityException se) {
            // Slow path when we are not trusted.
            try {
                Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                return (Unsafe) theUnsafe.get(Unsafe.class);
            } catch (Exception e) {
                throw new RuntimeException("exception while trying to get Unsafe", e);
            }
        }
    }

    public static AddressPointer allocateMemory(Unit.Bytes size) {
        return new AddressPointer(UNSAFE.allocateMemory(size.getBytes()));
    }

    public static <T> T allocateInstance(Class<T> clazz) throws InstantiationException {
        return (T) UNSAFE.allocateInstance(clazz);
    }

    public static void putArray(Object array, long address, long length) {
        long baseOffset = UNSAFE.arrayBaseOffset(array.getClass());
        long scaleFactor = UNSAFE.arrayIndexScale(array.getClass());
        putArray(array, address, length, baseOffset, scaleFactor);
    }

    public static void putArray(Object array, long address, long length, long offset, long scaleFactor) {
        UNSAFE.copyMemory(array, offset, null, address, length * scaleFactor);
    }

    public static void putNativeArray(Object array, int length, long address) {
        long baseOffset = UNSAFE.arrayBaseOffset(array.getClass());
        long scaleFactor = UNSAFE.arrayIndexScale(array.getClass());
        UNSAFE.copyMemory(array, 0, null, address, baseOffset + (length * scaleFactor));
    }

    public static void getArray(Object array, long address, long length) {
        long baseOffset = UNSAFE.arrayBaseOffset(array.getClass());
        long scaleFactor = UNSAFE.arrayIndexScale(array.getClass());
        getArray(array, address, length, baseOffset, scaleFactor);
    }

    public static void getArray(Object array, long address, long length, long offset, long scaleFactor) {
        UNSAFE.copyMemory(null, address, array, offset, length * scaleFactor);
    }


    public static int getInt(long address) {
        return UNSAFE.getInt(address);
    }

    public static int getIntVolatile(long address) {
        return UNSAFE.getIntVolatile(null, address);
    }

    public static char getChar(long address) {
        return UNSAFE.getChar(address);
    }

    public static byte getByte(long address) {
        return UNSAFE.getByte(address);
    }

    public static long getLong(long address) {
        return UNSAFE.getLong(address);
    }

    public static float getFloat(long address) {
        return UNSAFE.getFloat(address);
    }

    public static double getDouble(long address) {
        return UNSAFE.getDouble(address);
    }

    public static short getShort(long address) {
        return UNSAFE.getShort(address);
    }

    public static void putByte(long address, byte value) {
        UNSAFE.putByte(address, value);
    }

    public static void putChar(long address, char value) {
        UNSAFE.putChar(address, value);
    }

    public static void putShort(long address, short value) {
        UNSAFE.putShort(address, value);
    }

    public static void putInt(long address, int value) {
        UNSAFE.putInt(address, value);
    }

    public static void putLong(long address, long value) {
        UNSAFE.putLong(address, value);
    }

    public static void putFloat(long address, float value) {
        UNSAFE.putFloat(address, value);
    }

    public static void putDouble(long address, double value) {
        UNSAFE.putDouble(address, value);
    }


    public static boolean weakCompareAndSetInt(long address, int expectedBits, int floatToRawIntBits) {
        return UNSAFE.compareAndSwapInt(null, address, expectedBits, floatToRawIntBits);
       // return UNSAFE.weakCompareAndSetInt(null, address, expectedBits, floatToRawIntBits);
    }
}
