package de.tub.dima.babelfish.storage;

/**
 * Abstraction for operations on pointers.
 */
public final class AddressPointer {
    private final long address;

    public AddressPointer(long value) {
        this.address = value;
    }

    public long getAddress() {
        return address;
    }

    public boolean getBoolean() {
        return UnsafeUtils.getByte(address) == (byte) 1;
    }

    public void putBoolean(boolean value) {
        byte bvalue = value ? (byte) 1 : (byte) 0;
        UnsafeUtils.putByte(address, bvalue);
    }

    public long getLong() {
        return UnsafeUtils.getLong(address);
    }

    public void putLong(long value) {
        UnsafeUtils.putLong(address, value);
    }

    public void putInt(int value) {
        UnsafeUtils.putInt(address, value);
    }

    public int getInt() {
        return UnsafeUtils.getInt(address);
    }
    public int getIntVolatile() {
        return UnsafeUtils.getIntVolatile(address);
    }

    public void putChar(char value) {
        UnsafeUtils.putChar(address, value);
    }

    public char getChar() {
        return UnsafeUtils.getChar(address);
    }

    public short getShort() {
        return UnsafeUtils.getShort(address);
    }

    public void putShort(short value) {
        UnsafeUtils.putShort(address, value);
    }

    public byte getByte() {
        return UnsafeUtils.getByte(address);
    }

    public void putByte(byte value) {
        UnsafeUtils.putByte(address, value);
    }

    public void putFloat(float value) {
        UnsafeUtils.putFloat(address, value);
    }

    public float getFloat() {
        return UnsafeUtils.getFloat(address);
    }

    public void putDouble(double value) {
        UnsafeUtils.putDouble(address, value);
    }

    public double getDouble() {
        return UnsafeUtils.getDouble(address);
    }


    public AddressPointer add(Unit.Bytes bytes) {
        return box(this.address + bytes.getBytes());
    }

    public AddressPointer add(AddressPointer addressPointer) {
        return box(this.address + addressPointer.address);
    }

    public AddressPointer substract(Unit.Bytes bytes) {
        return box(this.address - bytes.getBytes());
    }

    public static AddressPointer box(long address) {
        return new AddressPointer(address);
    }
}
