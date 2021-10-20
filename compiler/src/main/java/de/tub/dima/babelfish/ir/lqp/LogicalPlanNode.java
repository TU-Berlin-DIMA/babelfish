package de.tub.dima.babelfish.ir.lqp;

import java.io.Serializable;

public interface LogicalPlanNode extends Serializable {

    default String getName() {
        if (this.getClass().isAnnotationPresent(Operator.class)) {
            return this.getClass().getAnnotation(Operator.class).name();
        }
        return this.getClass().getTypeName();
    }
}
