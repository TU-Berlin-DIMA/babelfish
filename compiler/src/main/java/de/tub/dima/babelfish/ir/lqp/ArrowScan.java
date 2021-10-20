package de.tub.dima.babelfish.ir.lqp;

@Operator(name = "Scan")
public class ArrowScan extends LogicalOperator {
    public final String catalogName;

    public ArrowScan(String catalogName) {
        this.catalogName = catalogName;
    }
}
