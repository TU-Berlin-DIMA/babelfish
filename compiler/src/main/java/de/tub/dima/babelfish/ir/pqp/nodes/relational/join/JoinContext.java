package de.tub.dima.babelfish.ir.pqp.nodes.relational.join;

import com.oracle.truffle.api.CompilerDirectives;
import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateContext;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;

/**
 * Context for BF Join operators
 */
public class JoinContext extends BFStateContext {

    private final long cardinality;

    @CompilerDirectives.CompilationFinal
    private PhysicalSchema physicalSchema;

    public JoinContext(long cardinality) {
        this.cardinality = cardinality;
    }

    public long getCardinality() {
        return cardinality;
    }

    public void addPhysicalSchema(PhysicalSchema physicalSchema) {
        this.physicalSchema = physicalSchema;
    }

    public PhysicalSchema getPhysicalSchema() {
        return physicalSchema;
    }
}
