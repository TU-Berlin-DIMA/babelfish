package de.tub.dima.babelfish.typesytem.tuple;

import de.tub.dima.babelfish.typesytem.record.*;

@LuthRecord
public final class Tuple2<T1, T2> implements Tuple {
    public T1 f1;
    public T2 f2;

    public Tuple2(T1 f1, T2 f2) {
        this.f1 = f1;
        this.f2 = f2;
    }
}
