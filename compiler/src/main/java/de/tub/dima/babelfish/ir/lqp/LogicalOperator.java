package de.tub.dima.babelfish.ir.lqp;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class LogicalOperator implements LogicalPlanNode {

    private final List<LogicalOperator> children;
    private final List<LogicalOperator> parents;


    protected LogicalOperator() {
        this.children = new ArrayList<>();
        this.parents = new ArrayList<>();
    }

    public void addChild(LogicalOperator logicalOperator) {
        this.children.add(logicalOperator);
        logicalOperator.parents.add(this);
    }

    public void addParent(LogicalOperator logicalOperator) {
        this.parents.add(logicalOperator);
        logicalOperator.children.add(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogicalOperator that = (LogicalOperator) o;
        return Objects.equals(children, that.children) &&
                Objects.equals(parents, that.parents);
    }

    public List<LogicalOperator> getChildren() {
        return children;
    }

    public List<LogicalOperator> getParents() {
        return parents;
    }

    public boolean hasChildren() {
        return !children.isEmpty();
    }
}
