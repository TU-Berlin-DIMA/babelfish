package de.tub.dima.babelfish.benchmark.parser;


import de.tub.dima.babelfish.benchmark.datatypes.Customer;
import de.tub.dima.babelfish.benchmark.datatypes.Lineitem;
import de.tub.dima.babelfish.benchmark.datatypes.Orders;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.layout.PhysicalLayoutFactory;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;

import java.io.IOException;
import java.util.List;

import static de.tub.dima.babelfish.benchmark.parser.SSBImporter.importTable;

public class TCPHImporter {

    public static void importTCPH(String path) throws IOException, SchemaExtractionException {
        PhysicalLayoutFactory factory = new PhysicalLayoutFactory.ColumnLayoutFactory();
        importTCPH(path, factory);
    }

    public static void importTCPH(String path, PhysicalLayoutFactory factory) throws IOException, SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();

        // import lineorder.tbl
        importTable(bufferManager, path + "lineitem.tbl", "table.lineitem", Lineitem.class, (List<String> fields) -> new Lineitem(
                Integer.valueOf(fields.get(0)),
                Integer.valueOf(fields.get(1)),
                Integer.valueOf(fields.get(2)),
                Integer.valueOf(fields.get(3)),
                new EagerNumeric((long) (Float.valueOf(fields.get(4)) * 100), 2),
                new EagerNumeric((long) (Float.valueOf(fields.get(5)) * 100), 2),
                new EagerNumeric((long) (Float.valueOf(fields.get(6)) * 100), 2),
                new EagerNumeric((long) (Float.valueOf(fields.get(7)) * 100), 2),
                new Char(fields.get(8).toCharArray()[0]),
                new Char(fields.get(9).toCharArray()[0]),
                new Date(fields.get(10)),
                new Date(fields.get(11)),
                new Date(fields.get(12))
        ), factory);


        // import orders.tbl
        importTable(bufferManager, path + "orders.tbl", "table.orders", Orders.class, (List<String> fields) -> new Orders(
                // o_orderkey
                Integer.parseInt(fields.get(0)),
                // o_custkey
                Integer.parseInt(fields.get(1)),
                // o_orderstatus
                new Char(fields.get(2).toCharArray()[0]),
                // o_totalprice
                Float.parseFloat(fields.get(3)),
                // o_orderdate
                new Date(fields.get(4)),
                // o_orderpriority
                new StringText(fields.get(5)),
                // o_clerk
                new StringText(fields.get(6)),
                // o_shippriority
                Integer.parseInt(fields.get(7)),
                // o_clerk
                new StringText(fields.get(8))
        ), factory);

        // import customer.tbl
        importTable(bufferManager, path + "customer.tbl", "table.customer", Customer.class, (List<String> fields) -> new Customer(
                // c_custkey
                Integer.valueOf(fields.get(0)),
                // c_name
                new StringText(fields.get(1)),
                // c_address
                new StringText(fields.get(2)),
                // c_nationkey
                Integer.valueOf(fields.get(3)),
                // c_phone
                new StringText(fields.get(4)),
                // c_acctbal
                Float.valueOf(fields.get(5)),
                // c_mktsegment
                new StringText(fields.get(6)),
                // c_comment
                new StringText(fields.get(7))
        ), factory);
    }

    /*

    public static void importTable(BufferManager bufferManager, String path, String tableName, Class<? extends Record> type, Function<List<String>, Record> parser) throws SchemaExtractionException, IOException {
        Schema schema = RecordUtil.createSchema(type);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        String cacheName = tableName + ".tmp";
        if (isCached(cacheName)) {
            registerTmpBuffer(bufferManager, cacheName, physicalSchema, tableName);
        } else {
            parseCSV(bufferManager, path, cacheName, schema, physicalSchema,tableName, parser);
        }
    }

    public static void parseCSV(BufferManager bufferManager, String path, String tmpPath, Schema schema, PhysicalSchema physicalSchema, String tableName, Function<List<String>, Record> parser) throws IOException {
        int recordsInFile = countLines(path);
        //int recordsInFile = 1000;
        FileReader fr = new FileReader(path);

        long bufferSize = ((((long) physicalSchema.getFixedRecordSize()) * recordsInFile) + 1024);
        System.out.println("Buffer Size: " + (bufferSize));
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(bufferSize));
        //PhysicalRowLayout physicalVariableLengthLayout = new PhysicalRowLayout(physicalSchema);
        PhysicalLayout physicalVariableLengthLayout = new PhysicalColumnLayout(physicalSchema, buffer.getSize().getBytes());
        physicalVariableLengthLayout.initBuffer(buffer);

        LineNumberReader br = new LineNumberReader(fr);
        String line = br.readLine();


        while (line != null && br.getLineNumber() < recordsInFile) {
            String[] fields = line.split("\\|");
            Record result = parser.apply(Arrays.asList(fields));
            GenericSerializer.serialize(schema, physicalVariableLengthLayout, buffer, result);
            line = br.readLine();
        }
        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        System.out.println("DUMP BUFFER TO TEMP");
        BufferDump.dumpBuffer(tmpPath, buffer);
        Catalog.getInstance().registerLayout(tableName, physicalVariableLengthLayout);
        Catalog.getInstance().registerBuffer(tableName, buffer);
        System.out.println("DUMP DONE");
    }

    public static void registerTmpBuffer(BufferManager bufferManager, String tempPath, PhysicalSchema physicalSchema, String tableName) {
        System.out.println("USE TMP file");
        Buffer buffer = BufferDump.readBuffer(tempPath, bufferManager);
        PhysicalLayout physicalVariableLengthLayout = new PhysicalColumnLayout(physicalSchema, buffer.getSize().getBytes());
        Catalog.getInstance().registerLayout(tableName, physicalVariableLengthLayout);
        Catalog.getInstance().registerBuffer(tableName, buffer);
        System.out.println("Reading temp file done ");
        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
    }

    public static boolean isCached(String path) {
        return new File(path).exists();
    }


    private static int countLines(String filename) throws IOException {
        LineNumberReader reader = new LineNumberReader(new FileReader(filename));
        int cnt = 0;
        String lineRead = "";
        while ((lineRead = reader.readLine()) != null) {
        }

        cnt = reader.getLineNumber();
        reader.close();
        return cnt;
    }

     */

}