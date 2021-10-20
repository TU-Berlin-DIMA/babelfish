package de.tub.dima.babelfish.ir.lqp;

import java.io.Serializable;

public class LogicalQueryPlan implements Serializable {

    private Sink sink;

    public LogicalQueryPlan(Scan sink) {
        //this.sink = sink;
    }

    public LogicalQueryPlan(Sink sink) {
        this.sink = sink;
    }

    public Sink getSource() {
        return sink;
    }
}
