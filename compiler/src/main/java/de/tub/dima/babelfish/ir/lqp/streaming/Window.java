package de.tub.dima.babelfish.ir.lqp.streaming;

import de.tub.dima.babelfish.ir.lqp.LogicalOperator;

public class Window extends LogicalOperator {
    private final WindowAssigner assigner;
    private final WindowTrigger trigger;
    private final WindowFunction function;


    public Window(WindowAssigner assigner, WindowTrigger trigger, WindowFunction function) {
        this.assigner = assigner;
        this.trigger = trigger;
        this.function = function;
    }
}
