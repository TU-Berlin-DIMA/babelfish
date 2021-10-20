package de.tub.dima.babelfish.ir.pqp.nodes.records;

import com.oracle.truffle.api.CompilerDirectives;
import de.tub.dima.babelfish.ir.pqp.nodes.BFBaseNode;
import de.tub.dima.babelfish.ir.pqp.objects.records.RecordSchema;

public class BFGetFieldIndexNode extends BFBaseNode {

    private final String field;

    @CompilerDirectives.CompilationFinal
    private int index;

    public BFGetFieldIndexNode(String field) {
        this.field = field;
    }

    public int getFieldIndex(RecordSchema schema) {
        if (CompilerDirectives.inInterpreter()) {
            index = schema.getFieldIndex(field);
        }
        return index;
    }

}
