package de.tub.dima.babelfish.typesytem.variableLengthType.Array;


public class PointerDoubleBFArray extends BFArray {

    private final long startAddress;

    public PointerDoubleBFArray(long startAddress, int length) {
        super(length);
        this.startAddress = startAddress;
    }

    public long getStartAddress() {
        return startAddress;
    }

    @Override
    public ComponentTypes getComponentType() {
        return ComponentTypes.Float_64;
    }
}
