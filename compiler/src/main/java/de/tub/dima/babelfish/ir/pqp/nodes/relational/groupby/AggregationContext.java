package de.tub.dima.babelfish.ir.pqp.nodes.relational.groupby;

import de.tub.dima.babelfish.ir.pqp.objects.state.BFStateContext;

public class AggregationContext extends BFStateContext {

    private final long cardinality;

    public AggregationContext(long cardinality) {
        this.cardinality = cardinality;
    }

    public long getCardinality() {
        return cardinality;
    }

}
