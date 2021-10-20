package de.tub.dima.babelfish.typesytem.variableLengthType.Array;

public class LazyExpBFArray extends BFArray {

    public final BFArray leftSource;

    protected LazyExpBFArray(BFArray leftSource, int length) {
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
