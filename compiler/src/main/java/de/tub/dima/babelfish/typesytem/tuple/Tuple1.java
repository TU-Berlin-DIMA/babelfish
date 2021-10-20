package de.tub.dima.babelfish.typesytem.tuple;

import de.tub.dima.babelfish.typesytem.record.*;

@LuthRecord
public final class Tuple1<T1> implements Tuple {
    public T1 f1;

    public Tuple1(T1 f1) {
        this.f1 = f1;
    }
}
