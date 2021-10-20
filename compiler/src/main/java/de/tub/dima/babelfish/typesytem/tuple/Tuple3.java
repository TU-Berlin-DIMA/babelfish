package de.tub.dima.babelfish.typesytem.tuple;

import de.tub.dima.babelfish.typesytem.record.*;

@LuthRecord
public final class Tuple3<T1, T2, T3> implements Tuple{
    public T1 f1;
    public T2 f2;
    public T3 f3;
}
