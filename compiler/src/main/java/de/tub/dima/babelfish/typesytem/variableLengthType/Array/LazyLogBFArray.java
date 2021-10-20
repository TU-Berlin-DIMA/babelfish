package de.tub.dima.babelfish.typesytem.variableLengthType.Array;

public class LazyLogBFArray extends BFArray {


    public final BFArray leftSource;

    protected LazyLogBFArray(BFArray leftSource, int length) {
        super(length);
        this.leftSource = leftSource;
    }

    @Override
    public ComponentTypes getComponentType() {
        return leftSource.getComponentType();
    }

    public BFArray getLeftSource() {
        return leftSource;
    }

}
