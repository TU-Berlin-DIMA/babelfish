package de.tub.dima.babelfish.benchmark.parser;

import de.tub.dima.babelfish.benchmark.tcph.BufferDump;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.layout.PhysicalCSVLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalLayout;
import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.record.RecordUtil;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.schema.Schema;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Precision;
import de.tub.dima.babelfish.typesytem.variableLengthType.MaxLength;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

import java.io.IOException;

public class TCPCSVImporter {


    public static void importTCPH(String path) throws IOException, SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        // import lineorder.tbl
        loadCSV(bufferManager, path + "lineitem.tbl", "table.lineitem", CSVLineitem.class, "|", false);
        loadCSV(bufferManager, path + "customer.tbl", "table.customer", CSVCustomer.class, "|", false);
        loadCSV(bufferManager, path + "orders.tbl", "table.orders", CSVOrders.class, "|", false);
    }

    public static void loadCSV(BufferManager bufferManager,
                               String path,
                               String tableName,
                               Class<? extends Record> type,
                               String slitter,
                               boolean skipHeader) throws SchemaExtractionException, IOException {
        Schema schema = RecordUtil.createSchema(type);
        String cacheName = tableName + ".tmp";

        loadCSV(bufferManager, path, cacheName, schema, tableName, slitter, skipHeader);

    }

    public static void loadCSV(BufferManager bufferManager,
                               String path,
                               String tmpPath,
                               Schema schema,
                               String tableName,
                               String slitter,
                               boolean skipHeader) throws IOException {

        Buffer buffer = BufferDump.readBuffer(path, bufferManager);

        PhysicalLayout physicalVariableLengthLayout = new PhysicalCSVLayout(schema);
        //physicalVariableLengthLayout.initBuffer(buffer);

        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        Catalog.getInstance().registerLayout(tableName, physicalVariableLengthLayout);
        Catalog.getInstance().registerBuffer(tableName, buffer);
        System.out.println("DUMP DONE");
    }

    @LuthRecord(name = "Lineitem")
    public class CSVLineitem implements Record {
        public int l_orderkey;
        public int l_partkey;
        public int l_suppkey;
        public int l_linenumber;
        @Precision(value = 2)
        public Numeric l_quantity;
        @Precision(value = 2)
        public Numeric l_extendedprice;
        @Precision(value = 2)
        public Numeric l_discount;
        @Precision(value = 2)
        public Numeric l_tax;
        public Char l_returnflag;
        public Char l_linestatus;
        public Date l_shipdate;
        public Date l_commitdate;
        public Date l_reciptdate;
        @MaxLength(length = 25)
        public Text shipinstruct;
        @MaxLength(length = 10)
        public Text shipmode;
        @MaxLength(length = 44)
        public Text comment;


        public CSVLineitem(int l_orderkey, int l_partkey, int l_suppkey, int l_linenumber, Numeric l_quantity, Numeric l_extendedprice, Numeric l_discount, Numeric l_tax, Char l_returnflag, Char l_linestatus, Date l_shipdate, Date l_commitdate, Date l_reciptdate) {
            this.l_orderkey = l_orderkey;
            this.l_partkey = l_partkey;
            this.l_suppkey = l_suppkey;
            this.l_linenumber = l_linenumber;
            this.l_quantity = l_quantity;
            this.l_extendedprice = l_extendedprice;
            this.l_discount = l_discount;
            this.l_tax = l_tax;
            this.l_returnflag = l_returnflag;
            this.l_linestatus = l_linestatus;
            this.l_shipdate = l_shipdate;
            this.l_commitdate = l_commitdate;
            this.l_reciptdate = l_reciptdate;
        }
    }

    @LuthRecord(name = "Customer")
    public class CSVCustomer implements Record {
        public int c_custkey;
        @MaxLength(length = 25)
        public Text c_name;
        @MaxLength(length = 40)
        public Text c_address;
        public int c_nationkey;
        @MaxLength(length = 15)
        public Text c_phone;
        @Precision(value = 2)
        public Numeric c_acctbal;
        @MaxLength(length = 10)
        public Text c_mktsegment;
        @MaxLength(length = 117)
        public Text c_comment;

        public CSVCustomer(int c_custkey, Text c_name, Text c_address, int c_nationkey, Text c_phone, Numeric c_acctbal, Text c_mktsegment, Text c_comment) {
            this.c_custkey = c_custkey;
            this.c_name = c_name;
            this.c_address = c_address;
            this.c_nationkey = c_nationkey;
            this.c_phone = c_phone;
            this.c_acctbal = c_acctbal;
            this.c_mktsegment = c_mktsegment;
            this.c_comment = c_comment;
        }
    }

    @LuthRecord(name = "Orders ")
    public class CSVOrders implements Record {
        public int o_orderkey;
        public int o_custkey;
        public Char o_orderstatus;
        @Precision(value = 2)
        public Numeric o_totalprice;
        public Date o_orderdate;
        @MaxLength(length = 15)
        public Text o_orderpriority;
        @MaxLength(length = 15)
        public Text o_clerk;
        public int o_shippriority;
        @MaxLength(length = 79)
        public Text o_comment;

        public CSVOrders(int o_orderkey, int o_custkey, Char o_orderstatus, Numeric o_totalprice, Date o_orderdate, Text o_orderpriority, Text o_clerk, int o_shippriority, Text o_comment) {
            this.o_orderkey = o_orderkey;
            this.o_custkey = o_custkey;
            this.o_orderstatus = o_orderstatus;
            this.o_totalprice = o_totalprice;
            this.o_orderdate = o_orderdate;
            this.o_orderpriority = o_orderpriority;
            this.o_clerk = o_clerk;
            this.o_shippriority = o_shippriority;
            this.o_comment = o_comment;
        }
    }

}
