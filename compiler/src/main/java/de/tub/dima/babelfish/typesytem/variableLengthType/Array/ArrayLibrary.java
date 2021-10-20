package de.tub.dima.babelfish.typesytem.variableLengthType.Array;

import com.oracle.truffle.api.library.GenerateLibrary;
import com.oracle.truffle.api.library.Library;

@GenerateLibrary()
public abstract class ArrayLibrary extends Library {

    public abstract Object read(BFArray array, int index);

    public abstract void write(BFArray array, int index, Object value);

    public abstract int length(BFArray array);

    public abstract Object sum(BFArray array);

    public abstract BFArray dotArray(BFArray array1, BFArray array2);

    public abstract BFArray addArray(BFArray array1, BFArray array2);

    public abstract BFArray subArray(BFArray array1, BFArray array2);

    public abstract BFArray divArray(BFArray array1, BFArray array2);

    public abstract BFArray dotScalar(BFArray array1, double scalar);

    public abstract BFArray addScalar(BFArray array1, double scalar);

    public abstract BFArray subScalarLeft(BFArray array1, double scalar);

    public abstract BFArray sqrt(BFArray array1);

    public abstract BFArray log(BFArray array1);

    public abstract BFArray erf(BFArray array1);

    public abstract BFArray exp(BFArray array1);


}
