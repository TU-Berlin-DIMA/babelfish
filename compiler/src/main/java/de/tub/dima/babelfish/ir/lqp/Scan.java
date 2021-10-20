package de.tub.dima.babelfish.ir.lqp;

@Operator(name = "Scan")
public class Scan extends LogicalOperator {
    public final String catalogName;

    public Scan(String catalogName) {
        this.catalogName = catalogName;
    }
}
