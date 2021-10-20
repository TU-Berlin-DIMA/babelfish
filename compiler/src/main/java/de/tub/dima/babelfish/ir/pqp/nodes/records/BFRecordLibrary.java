package de.tub.dima.babelfish.ir.pqp.nodes.records;

import com.oracle.truffle.api.library.GenerateLibrary;
import com.oracle.truffle.api.library.Library;
import de.tub.dima.babelfish.ir.pqp.objects.records.BFRecord;
import de.tub.dima.babelfish.typesytem.BFType;

@GenerateLibrary()
@GenerateLibrary.DefaultExport(BFRecordBuiltins.class)
public abstract class BFRecordLibrary extends Library {

    public abstract void setValue(BFRecord obj, String fieldName, BFType value);

    public abstract BFType getValue(BFRecord obj, String fieldName);

}
