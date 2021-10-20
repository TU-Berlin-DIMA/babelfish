package de.tub.dima.babelfish.ir.lqp.relational;

import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Operator;

@Operator(name = "Selection")
public class Selection extends LogicalOperator {

    private final Predicate[] predicates;

    public Selection(Predicate... predicates) {
        this.predicates = predicates;
    }

    public Predicate[] getPredicates() {
        return predicates;
    }
}
