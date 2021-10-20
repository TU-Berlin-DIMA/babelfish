package de.tub.dima.babelfish.ir.pqp.objects.polyglot.pandas;

import com.oracle.truffle.api.library.GenerateLibrary;
import com.oracle.truffle.api.library.Library;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;

@GenerateLibrary()
public abstract class PandasDataframeLibrary  extends Library {
    public abstract void open(PandasDataframeWrapper dataframeWrapper);
    public abstract void execute(PandasDataframeWrapper dataframeWrapper, BFRecord wrapper);
}
