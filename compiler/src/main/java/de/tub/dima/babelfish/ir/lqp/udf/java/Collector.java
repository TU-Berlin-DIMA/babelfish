package de.tub.dima.babelfish.ir.lqp.udf.java;

import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.typesytem.record.Record;

public interface Collector<T extends Record> {
    void emit(VirtualFrame frame, T object);
}
