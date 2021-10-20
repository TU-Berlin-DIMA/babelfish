package de.tub.dima.babelfish.ir.lqp.relational;

import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Operator;

@Operator(name = "GroupBy")
public class GroupBy extends LogicalOperator {

    private Aggregation[] aggregations;
    private KeyGroup keys;
    private long cardinality = -1;

    public GroupBy(Aggregation... aggregations) {
        this.aggregations = aggregations;
    }

    public GroupBy(KeyGroup key, Aggregation... aggregations) {
        this.aggregations = aggregations;
        keys = key;
    }

    public GroupBy(KeyGroup key, long cardinality, Aggregation... aggregations) {
        this.aggregations = aggregations;
        keys = key;
        this.cardinality = cardinality;
    }

    public void addAggregation(Aggregation aggregation) {
        //aggregations.add(aggregation);
    }

    public KeyGroup getKeys() {
        return keys;
    }

    public boolean hasKeys() {
        return keys != null;
    }

    public Aggregation[] getAggregations() {
        return aggregations;
    }

    public long getCardinality() {
        return cardinality;
    }
}
