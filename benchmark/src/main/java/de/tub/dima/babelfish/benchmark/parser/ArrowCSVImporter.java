package de.tub.dima.babelfish.benchmark.parser;

import de.tub.dima.babelfish.benchmark.datatypes.Lineitem;
import de.tub.dima.babelfish.benchmark.tcph.BufferDump;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.layout.*;
import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.record.RecordUtil;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.schema.Schema;
import de.tub.dima.babelfish.typesytem.schema.field.DateField;
import de.tub.dima.babelfish.typesytem.schema.field.NumericField;
import de.tub.dima.babelfish.typesytem.schema.field.SchemaField;
import de.tub.dima.babelfish.typesytem.schema.field.TextField;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_16;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_8;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Precision;
import de.tub.dima.babelfish.typesytem.variableLengthType.MaxLength;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

public class ArrowCSVImporter {


    public static void importArrow(String path, RootAllocator allocator) throws IOException, SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        // import lineorder.tbl
        importTable(bufferManager, path + "lineitem.tbl", "table.lineitem", CSVLineitem.class, new BiConsumer<List<String>, List<FieldVector>>() {
            @Override
            public void accept(List<String> fields, List<FieldVector> fieldVectors) {
                int currentCount = fieldVectors.get(0).getValueCount();
                IntVector field_0 = (IntVector) fieldVectors.get(0);
                field_0.setSafe(currentCount, Integer.valueOf(fields.get(0)));
                fieldVectors.get(0).setValueCount(currentCount + 1);
                IntVector field_1 = (IntVector) fieldVectors.get(1);
                field_1.setSafe(currentCount, Integer.valueOf(fields.get(1)));

                IntVector field_2 = (IntVector) fieldVectors.get(2);
                field_2.setSafe(currentCount, Integer.valueOf(fields.get(2)));

                IntVector field_3 = (IntVector) fieldVectors.get(3);
                field_3.setSafe(currentCount, Integer.valueOf(fields.get(3)));

                DecimalVector field_4 = (DecimalVector) fieldVectors.get(4);
                field_4.setSafe(currentCount, (long) (Float.valueOf(fields.get(4)) * 100));

                DecimalVector field_5 = (DecimalVector) fieldVectors.get(5);
                field_5.setSafe(currentCount, (long) (Float.valueOf(fields.get(5)) * 100));

                DecimalVector field_6 = (DecimalVector) fieldVectors.get(6);
                field_6.setSafe(currentCount, (long) (Float.valueOf(fields.get(6)) * 100));

                DecimalVector field_7 = (DecimalVector) fieldVectors.get(7);
                field_7.setSafe(currentCount, (long) (Float.valueOf(fields.get(7)) * 100));

                TimeMilliVector field_10 = (TimeMilliVector) fieldVectors.get(10);
                field_10.setSafe(currentCount, Date.parse(fields.get(10)));

                TimeMilliVector field_11 = (TimeMilliVector) fieldVectors.get(11);
                field_11.setSafe(currentCount, Date.parse(fields.get(11)));

                TimeMilliVector field_12 = (TimeMilliVector) fieldVectors.get(12);
                field_12.setSafe(currentCount, Date.parse(fields.get(12)));

                VarCharVector field_13 = (VarCharVector) fieldVectors.get(13);
                field_13.setSafe(currentCount, new org.apache.arrow.vector.util.Text(fields.get(13)));

                VarCharVector field_14 = (VarCharVector) fieldVectors.get(14);
                field_14.setSafe(currentCount, new org.apache.arrow.vector.util.Text(fields.get(14)));

                VarCharVector field_15 = (VarCharVector) fieldVectors.get(15);
                field_15.setSafe(currentCount, new org.apache.arrow.vector.util.Text(fields.get(15)));


            }
        }, "\\|", false, allocator);

        importTable(bufferManager, path + "customer.tbl", "table.customer", CSVCustomer.class, new BiConsumer<List<String>, List<FieldVector>>() {
            @Override
            public void accept(List<String> fields, List<FieldVector> fieldVectors) {
                int currentCount = fieldVectors.get(0).getValueCount();
                IntVector field_0 = (IntVector) fieldVectors.get(0);
                field_0.setSafe(currentCount, Integer.valueOf(fields.get(0)));
                fieldVectors.get(0).setValueCount(currentCount + 1);
                // c_name
                VarCharVector field_1 = (VarCharVector) fieldVectors.get(1);
                field_1.setSafe(currentCount, new org.apache.arrow.vector.util.Text(fields.get(1)));

                // c_address
                VarCharVector field_2 = (VarCharVector) fieldVectors.get(2);
                field_2.setSafe(currentCount, new org.apache.arrow.vector.util.Text(fields.get(2)));
                // c_nationke
                IntVector field_3 = (IntVector) fieldVectors.get(3);
                field_3.setSafe(currentCount, Integer.valueOf(fields.get(3)));
                // c_phone
                VarCharVector field_4 = (VarCharVector) fieldVectors.get(4);
                field_4.setSafe(currentCount, new org.apache.arrow.vector.util.Text(fields.get(4)));
                // c_acctbal
                DecimalVector field_5 = (DecimalVector) fieldVectors.get(5);
                field_5.setSafe(currentCount, (long) (Float.valueOf(fields.get(5)) * 100));
                // c_mktsegment
                VarCharVector field_6 = (VarCharVector) fieldVectors.get(6);
                field_6.setSafe(currentCount, new org.apache.arrow.vector.util.Text(new StringText(fields.get(6), 10).toString()));
                // c_comment
                VarCharVector field_7 = (VarCharVector) fieldVectors.get(7);
                field_7.setSafe(currentCount, new org.apache.arrow.vector.util.Text(fields.get(7)));
            }
        }, "\\|", false, allocator);

        importTable(bufferManager, path + "orders.tbl", "table.orders", CSVOrders.class, new BiConsumer<List<String>, List<FieldVector>>() {
            @Override
            public void accept(List<String> fields, List<FieldVector> fieldVectors) {


                // o_orderkey
                int currentCount = fieldVectors.get(0).getValueCount();
                IntVector field_0 = (IntVector) fieldVectors.get(0);
                field_0.setSafe(currentCount, Integer.valueOf(fields.get(0)));
                fieldVectors.get(0).setValueCount(currentCount + 1);
                // o_custkey
                IntVector field_1 = (IntVector) fieldVectors.get(1);
                field_1.setSafe(currentCount, Integer.valueOf(fields.get(1)));
                // o_orderstatus
                VarCharVector field_2 = (VarCharVector) fieldVectors.get(2);
                field_2.setSafe(currentCount, new org.apache.arrow.vector.util.Text(fields.get(2)));
                // o_totalprice
                DecimalVector field_3 = (DecimalVector) fieldVectors.get(3);
                field_3.setSafe(currentCount, (long) (Float.valueOf(fields.get(3)) * 100));

                // o_orderdate
                TimeMilliVector field_4 = (TimeMilliVector) fieldVectors.get(4);
                field_4.setSafe(currentCount, Date.parse(fields.get(4)));

                // o_orderpriority
                VarCharVector field_5 = (VarCharVector) fieldVectors.get(5);
                field_5.setSafe(currentCount, new org.apache.arrow.vector.util.Text(fields.get(5)));

                // o_clerk
                VarCharVector field_6 = (VarCharVector) fieldVectors.get(6);
                field_6.setSafe(currentCount, new org.apache.arrow.vector.util.Text(fields.get(6)));

                // o_shippriority
                IntVector field_7 = (IntVector) fieldVectors.get(7);
                field_7.setSafe(currentCount, Integer.valueOf(fields.get(7)));
                // o_clerk
                VarCharVector field_8 = (VarCharVector) fieldVectors.get(8);
                field_8.setSafe(currentCount, new org.apache.arrow.vector.util.Text(fields.get(8)));


            }
        }, "\\|", false, allocator);
    }

    public static void importTable(BufferManager bufferManager,
                                   String path,
                                   String tableName,
                                   Class<? extends Record> type,
                                   BiConsumer<List<String>, List<FieldVector>> parser,
                                   String slitter,
                                   boolean skipHeader, RootAllocator allocator) throws SchemaExtractionException, IOException {
        Schema schema = RecordUtil.createSchema(type);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        String cacheName = tableName + ".tmp";
        // if (isCached(cacheName)) {
        //     registerTmpBuffer(bufferManager, cacheName, physicalSchema, tableName, factory);
        //} else {
        parseCSV(bufferManager, path, cacheName, schema, physicalSchema, tableName, parser, slitter, skipHeader, allocator);
        //}
    }

    public static List<FieldVector> getFields(Schema schema, RootAllocator allocator) {
        List<FieldVector> fields = new ArrayList<>();
        for (SchemaField schemaField : schema.getFields()) {
            fields.add(getField(schemaField, allocator));
        }
        return fields;
    }

    public static FieldVector getField(SchemaField field, RootAllocator allocator) {

        if (field instanceof NumericField) {
            return new DecimalVector(field.getName(), allocator, ((NumericField) field).getPrecission(), ((NumericField) field).getPrecission());
        } else if (field instanceof DateField) {
            return new TimeMilliVector(field.getName(), allocator);
        } else if (field instanceof TextField) {
            return new VarCharVector(field.getName(), allocator);
        } else if (Char.class.isAssignableFrom(field.getType())) {
            return new VarCharVector(field.getName(), allocator);
        } else if (Int_8.class.isAssignableFrom(field.getType())) {
            return new TinyIntVector(field.getName(), allocator);
        } else if (Int_16.class.isAssignableFrom(field.getType())) {
            return new SmallIntVector(field.getName(), allocator);
        } else if (Int_32.class.isAssignableFrom(field.getType())) {
            return new IntVector(field.getName(), allocator);
        } else if (Int_64.class.isAssignableFrom(field.getType())) {
            return new BigIntVector(field.getName(), allocator);
        } else {
            return null;
        }

    }

    public static void parseCSV(BufferManager bufferManager, String path, String tmpPath, Schema schema, PhysicalSchema
            physicalSchema, String tableName, BiConsumer<List<String>, List<FieldVector>> parser, String slitter, boolean skipHeader, RootAllocator allocator) throws IOException {
        int recordsInFile = countLines(path);
        //int recordsInFile = 1000;
        FileReader fr = new FileReader(path);


        //RootAllocator allocator = new RootAllocator();


        List<FieldVector> fields = getFields(schema, allocator);


        LineNumberReader br = new LineNumberReader(fr);
        String line = br.readLine();
        if (skipHeader)
            line = br.readLine();

        while (line != null && br.getLineNumber() < recordsInFile) {
            String[] stringFields = line.split(slitter);
            parser.accept(Arrays.asList(stringFields), fields);
            line = br.readLine();
        }
        List<Field> arrowFields = new ArrayList<>();
        List<FieldVector> arrowFieldVectors = new ArrayList<>();
        for (FieldVector fieldVector : fields) {
            if (fieldVector != null) {
                fieldVector.setValueCount(recordsInFile - 1);
                arrowFields.add(fieldVector.getField());
                arrowFieldVectors.add(fieldVector);
            }
        }

        VectorSchemaRoot schemaRoot = new VectorSchemaRoot(arrowFields, arrowFieldVectors);

        //ByteArrayOutputStream out = new ByteArrayOutputStream();
        //ArrowStreamWriter writer = new ArrowStreamWriter(
        //       schemaRoot,
        //       null,
        //       Channels.newChannel(out));
        //writer.start();
        //writer.writeBatch();
        //writer.end();


        System.out.println("DUMP BUFFER TO TEMP");

        //Catalog.getInstance().registerLayout(tableName, physicalVariableLengthLayout);
        Catalog.getInstance().registerArrowLayout(tableName, schemaRoot);
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
