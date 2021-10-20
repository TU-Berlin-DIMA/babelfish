package de.tub.dima.babelfish.ir.pqp.nodes.relational.sinks;

import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;

public interface InputReader {

    boolean hasNext();

    void next(BFRecord object);


}
