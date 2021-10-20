package de.tub.dima.babelfish.ir.lqp.udf;

import com.oracle.truffle.api.frame.VirtualFrame;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.typesytem.record.DynamicRecord;

public abstract class UDFOperator<T extends UDF> extends LogicalOperator {

    protected final T udf;

    public UDFOperator(T udf) {
        this.udf = udf;
    }

    public T getUdf() {
        return udf;
    }


    public abstract void execute(VirtualFrame frame, DynamicRecord input, OutputCollector outputCollector);

    public abstract void init(OutputCollector outputCollector);

    public interface OutputCollector {
        DynamicRecord createOutputRecord();

        void emitRecord(VirtualFrame frame, DynamicRecord dynamicRecord);
    }
}
