package de.tub.dima.babelfish.benchmark.parser;

import de.tub.dima.babelfish.benchmark.datatypes.*;
import de.tub.dima.babelfish.benchmark.tcph.BufferDump;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.layout.*;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.record.RecordUtil;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.schema.Schema;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class SSBImporter {

    public static void importSSB(String path) throws IOException, SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        PhysicalLayoutFactory factory = new PhysicalLayoutFactory.ColumnLayoutFactory();
        // import lineorder.tbl
        importTable(bufferManager, path + "lineorder.tbl", "table.sbb_lineorder", SSB_LINEORDER.class, (List<String> fields) -> new SSB_LINEORDER(
                // lo_orderkey
                Integer.valueOf(fields.get(0)),
                // lo_linenumber
                Integer.valueOf(fields.get(1)),
                // lo_custkey
                Integer.valueOf(fields.get(2)),
                // lo_partkey
                Integer.valueOf(fields.get(3)),
                // lo_suppkey
                Integer.valueOf(fields.get(4)),
                // lo_orderdate
                Integer.valueOf(fields.get(5)),
                // lo_orderpriority
                // Integer.valueOf(fields.get(6)),
                // lo_shippriority
                new Char(fields.get(7).toCharArray()[0]),
                // lo_quantity
                Integer.valueOf(fields.get(8)),
                // lo_extendedprice
                new EagerNumeric((long) (Integer.parseInt(fields.get(9)) * 100), 2),
                // lo_ordtotalprice
                new EagerNumeric((long) (Integer.parseInt(fields.get(10)) * 100), 2),
                // lo_discount
                new EagerNumeric((long) (Integer.parseInt(fields.get(11)) * 100), 2),
                // lo_revenue
                new EagerNumeric((long) (Integer.parseInt(fields.get(12)) * 100), 2),
                // lo_supplycost
                new EagerNumeric((long) (Integer.parseInt(fields.get(13)) * 100), 2),
                // lo_tax
                Integer.valueOf(fields.get(14))
        ), factory);

        // import lineorder.tbl
        importTable(bufferManager, path + "date.tbl", "table.sbb_date", SBB_DATE.class, (List<String> fields) -> new SBB_DATE(
                // d_datekey
                Integer.valueOf(fields.get(0)),
                // d_date
                // d_dayofweek
                // d_month
                // d_year
                Integer.valueOf(fields.get(4)),
                // d_yearmonthnum
                Integer.valueOf(fields.get(5)),
                // d_yearmonth
                // d_daynuminweek
                Integer.valueOf(fields.get(7)),
                // d_daynuminmonth
                Integer.valueOf(fields.get(8)),
                // d_daynuminyear
                Integer.valueOf(fields.get(9)),
                // d_monthnuminyear
                Integer.valueOf(fields.get(10)),
                // d_weeknuminyear
                Integer.valueOf(fields.get(11)),
                // d_sellingseasin
                // d_lastdayinweekfl
                Integer.valueOf(fields.get(13)),
                // d_lastdayinmonthfl
                Integer.valueOf(fields.get(14)),
                // d_holidayfl
                Integer.valueOf(fields.get(15)),
                // d_weekdayfl
                Integer.valueOf(fields.get(16))
        ), factory);

        // import part.tbl
        importTable(bufferManager, path + "part.tbl", "table.sbb_part", SSB_PART.class, (List<String> fields) -> new SSB_PART(
                // p_partkey
                Integer.valueOf(fields.get(0)),
                // p_name
                new StringText(fields.get(1)),
                // p_mfgr
                new StringText(fields.get(2)),
                // p_category
                new StringText(fields.get(3)),
                // p_brand1
                new StringText(fields.get(4)),
                // p_color
                new StringText(fields.get(5)),
                // p_type
                new StringText(fields.get(6)),
                // p_size
                Integer.valueOf(fields.get(7)),
                // p_container
                new StringText(fields.get(8))
        ), factory);

        // import supplier.tbl
        importTable(bufferManager, path + "supplier.tbl", "table.sbb_supplier", SSB_SUPPLIER.class, (List<String> fields) -> new SSB_SUPPLIER(
                // s_suppkey
                Integer.valueOf(fields.get(0)),
                // s_name
                new StringText(fields.get(1)),
                // s_address
                new StringText(fields.get(2)),
                // s_city
                new StringText(fields.get(3)),
                // s_nation
                new StringText(fields.get(4)),
                // s_region
                new StringText(fields.get(5)),
                // s_phone
                new StringText(fields.get(6))
        ), factory);

        // import supplier.tbl
        importTable(bufferManager, path + "customer.tbl", "table.sbb_customer", SSB_CUSTOMER.class, (List<String> fields) -> new SSB_CUSTOMER(
                // s_suppkey
                Integer.valueOf(fields.get(0)),
                // s_name
                new StringText(fields.get(1)),
                // s_address
                new StringText(fields.get(2)),
                // s_city
                new StringText(fields.get(3)),
                // s_nation
                new StringText(fields.get(4)),
                // s_region
                new StringText(fields.get(5)),
                // s_phone
                new StringText(fields.get(6)),
                new StringText(fields.get(7))
        ), factory);
    }

    public static void importTable(BufferManager bufferManager, String path, String tableName, Class<? extends Record> type, Function<List<String>, Record> parser, String slitter, boolean skipHeader, PhysicalLayoutFactory factory) throws SchemaExtractionException, IOException {
        Schema schema = RecordUtil.createSchema(type);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        String cacheName = tableName + "_" + factory.getName() + ".tmp";

        if (isCached(cacheName)) {
            registerTmpBuffer(bufferManager, cacheName, physicalSchema, tableName, factory);
        } else {
            parseCSV(bufferManager, path, cacheName, schema, physicalSchema, tableName, parser, slitter, skipHeader, factory);
        }
    }

    public static void importTable(BufferManager bufferManager, String path, String tableName, Class<? extends Record> type, Function<List<String>, Record> parser, PhysicalLayoutFactory factory) throws SchemaExtractionException, IOException {
        importTable(bufferManager, path, tableName, type, parser, "\\|", false, factory);
    }

    public static void parseCSV(BufferManager bufferManager, String path, String tmpPath, Schema schema, PhysicalSchema
            physicalSchema, String tableName, Function<List<String>, Record> parser, String slitter, boolean skipHeader,
                                PhysicalLayoutFactory factory) throws IOException {
        int recordsInFile = countLines(path);
        //int recordsInFile = 1000;
        FileReader fr = new FileReader(path);

        long bufferSize = ((((long) physicalSchema.getFixedRecordSize()) * recordsInFile) + 1024);
        System.out.println("Buffer Size: " + (bufferSize));
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(bufferSize));
        PhysicalLayout physicalVariableLengthLayout = factory.create(physicalSchema, buffer.getSize().getBytes());
        physicalVariableLengthLayout.initBuffer(buffer);

        LineNumberReader br = new LineNumberReader(fr);
        String line = br.readLine();
        if (skipHeader)
            line = br.readLine();

        while (line != null && br.getLineNumber() < recordsInFile) {
            String[] fields = line.split(slitter);
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

    public static void registerTmpBuffer(BufferManager bufferManager, String tempPath, PhysicalSchema physicalSchema, String tableName,
                                         PhysicalLayoutFactory factory) {
        System.out.println("USE TMP file");
        Buffer buffer = BufferDump.readBuffer(tempPath, bufferManager);
        PhysicalLayout physicalVariableLengthLayout = factory.create(physicalSchema, buffer.getSize().getBytes());
        Catalog.getInstance().registerLayout(tableName, physicalVariableLengthLayout);
        Catalog.getInstance().registerBuffer(tableName, buffer);
        System.out.println("Reading temp file done ");
        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
    }

    public static boolean isCached(String path) {
        return new File(path).exists();
    }

    public static Buffer readFile(String path) throws IOException, SchemaExtractionException {

        BufferManager bufferManager = new BufferManager();
        Schema lineitemSchema = RecordUtil.createSchema(Lineitem.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(lineitemSchema).build();

        if (new File("lineitem.tmp").exists()) {
            System.out.println("USE TMP file");
            Buffer buffer = BufferDump.readBuffer("lineitem.tmp", bufferManager);
            PhysicalLayout physicalVariableLengthLayout = new PhysicalColumnLayout(physicalSchema, buffer.getSize().getBytes());
            Catalog.getInstance().registerLayout("table.lineitem", physicalVariableLengthLayout);
            System.out.println("Reading temp file done ");
            System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
            return buffer;
        }


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
            /*Lineitem li = new Lineitem(
                    Integer.valueOf(fields[0]),
                    Integer.valueOf(fields[1]),
                    Integer.valueOf(fields[2]),
                    Integer.valueOf(fields[3]),
                    Float.valueOf(fields[4]),
                    Float.valueOf(fields[5]),
                    Float.valueOf(fields[6]),
                    Float.valueOf(fields[7]),
                    new Char(fields[8].toCharArray()[0]),
                    new Char(fields[9].toCharArray()[0]),
                    new Date(fields[10]),
                    new Date(fields[11]),
                    new Date(fields[12]),
                    null,
                    null,
                    null
            );
            */
            GenericSerializer.serialize(lineitemSchema, physicalVariableLengthLayout, buffer, null
            );
            line = br.readLine();
        }
        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        System.out.println("DUMP BUFFER TO TEMP");
        BufferDump.dumpBuffer("lineitem.tmp", buffer);
        System.out.println("DUMP DONE");

        return buffer;
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

}
