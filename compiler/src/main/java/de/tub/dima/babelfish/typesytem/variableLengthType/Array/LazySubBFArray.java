package de.tub.dima.babelfish.typesytem.variableLengthType.Array;

public class LazySubBFArray extends BFArray {


    public final BFArray leftSource;
    public final BFArray rightSource;

    protected LazySubBFArray(BFArray leftSource, BFArray rightSource, int length) {
        super(length);
        this.leftSource = leftSource;
        this.rightSource = rightSource;
    }

    @Override
    public ComponentTypes getComponentType() {
        return leftSource.getComponentType();
    }

    public BFArray getLeftSource() {
        return leftSource;
    }

    public BFArray getRightSource() {
        return rightSource;
    }
}
