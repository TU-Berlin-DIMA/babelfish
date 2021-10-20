package de.tub.dima.babelfish.ir.lqp;

@Operator(name = "Scan")
public class CSVScan extends LogicalOperator {
    public final String catalogName;

    public CSVScan(String catalogName) {
        this.catalogName = catalogName;
    }
}
