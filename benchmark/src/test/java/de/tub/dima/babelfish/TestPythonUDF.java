package de.tub.dima.babelfish;

import de.tub.dima.babelfish.benchmark.parser.AirlineImporter;
import de.tub.dima.babelfish.benchmark.parser.CrimeIndexImporter;
import de.tub.dima.babelfish.benchmark.parser.TCPHImporter;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.LogicalQueryPlan;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.Projection;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptOperator;
import de.tub.dima.babelfish.ir.lqp.udf.js.JavaScriptUDF;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonPandasUDF;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonScalarUDF;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonUDF;
import de.tub.dima.babelfish.storage.*;
import de.tub.dima.babelfish.storage.layout.PhysicalLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalLayoutFactory;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;
import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.record.RecordUtil;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.schema.Schema;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.variableLengthType.MaxLength;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.ByteSequence;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Random;

import static de.tub.dima.babelfish.benchmark.tcph.queries.Query6.pythonTCPHUDFQuery;

public class TestPythonUDF {

    private Buffer buffer;
    private BufferManager outputBufferManager;

    @Before
    public void setup() throws IOException, SchemaExtractionException {
        System.setProperty("luth.home", ".");
        System.setProperty("js.home", ".");

        outputBufferManager = new BufferManager();
        Catalog.getInstance().getLayout("lineitem");
    }


    public static LogicalOperator javaScriptReturnUDF_Exception() {
        Scan scan = new Scan("table.lineitem");
        JavaScriptOperator selection = new JavaScriptOperator(new JavaScriptUDF("(x)=>{return 10;}"));
        scan.addChild(selection);
        Sink sink = new Sink.PrintSink();
        selection.addChild(sink);
        return scan;
    }

    public static LogicalOperator pythonReturnUDF() {

        Scan scan = new Scan("table.lineitem");
        //PythonOperator selection = new PythonOperator(new PythonUDF("def test(a) : (a.l_discount*10)\n"));
        PythonOperator selection = new PythonOperator(new PythonScalarUDF("def udf(rec,ctx):\n" +
                "\treturn rec.l_shipdate<rec.l_shipdate" +
                "\nlambda rec,ctx: udf(rec,ctx)"));
        scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }


    public static LogicalOperator pythonCallbackUDF() {
        Scan scan = new Scan("table.lineitem");
        //PythonOperator selection = new PythonOperator(new PythonUDF("def test(a) : (a.l_discount*10)\n"));
        PythonOperator selection = new PythonOperator(new PythonUDF("lambda a,ctx: ctx(a)"));
        scan.addChild(selection);
        Sink sink = new Sink.PrintSink();
        selection.addChild(sink);
        return scan;
    }


    @Test
    public void executePhysonTPCHUDF() throws IOException, InterruptedException, SchemaExtractionException {

        Sink scan = (Sink) pythonTCPHUDFQuery();
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan(scan));
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }

    @LuthRecord(name = "arrayValue")
    public class ArrayValue implements Record {

        public ArrayValue(double[] value) {
            this.value = value;
        }

        @MaxLength(length = 100)
        public double[] value;
    }

    @Test
    public void executeNumpyUDF1() throws IOException, InterruptedException, SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        int size = 10000;
        Schema schema = RecordUtil.createSchema(ArrayValue.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long bufferSize = ((((long) physicalSchema.getFixedRecordSize()) * size) + 1024);
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(bufferSize));
        PhysicalLayoutFactory.ColumnLayoutFactory fac = new PhysicalLayoutFactory.ColumnLayoutFactory();
        PhysicalLayout physicalVariableLengthLayout = fac.create(physicalSchema, buffer.getSize().getBytes());
        physicalVariableLengthLayout.initBuffer(buffer);
        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");

        for (long records = 0; records < size; records++) {
            long recordOffset = physicalVariableLengthLayout.getFieldBufferOffset(records, 0).getAddress();
            for (int i = 0; i < 100; i++) {
                int arrayValueOffset = i * 8;
                UnsafeUtils.putDouble(buffer.getVirtualAddress().getAddress() +
                        recordOffset + arrayValueOffset, i);
            }
            physicalVariableLengthLayout.incrementRecordNumber(buffer);
        }

        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        Catalog.getInstance().registerLayout("table.array", physicalVariableLengthLayout);
        Catalog.getInstance().registerBuffer("table.array", buffer);

        Scan scan = new Scan("table.array");
        PythonOperator selection = new PythonOperator(new PythonScalarUDF("" +
                "import site\n" +
                "import numpy as np\n" +
                "def udf(rec):\n" +
                "\tbuf2 = rec.value.toBuffer()\n" +
                "\tx=np.frombuffer(buf2)\n" +
                "\treturn x.sum().item()\n" +
                "\nlambda rec,ctx: udf(rec)"));
        scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);

        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan(sink));
        Thread.sleep(1000);
        for (int i = 0; i < 10000; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }

    @Test
    public void executeNumpyUDFSumFromConstantArray() throws IOException, InterruptedException, SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        int size = 10000;
        Schema schema = RecordUtil.createSchema(ArrayValue.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long bufferSize = ((((long) physicalSchema.getFixedRecordSize()) * size) + 1024);
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(bufferSize));
        PhysicalLayoutFactory.ColumnLayoutFactory fac = new PhysicalLayoutFactory.ColumnLayoutFactory();
        PhysicalLayout physicalVariableLengthLayout = fac.create(physicalSchema, buffer.getSize().getBytes());
        physicalVariableLengthLayout.initBuffer(buffer);
        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");

        for (long records = 0; records < size; records++) {
            long recordOffset = physicalVariableLengthLayout.getFieldBufferOffset(records, 0).getAddress();
            for (int i = 0; i < 100; i++) {
                int arrayValueOffset = i * 8;
                UnsafeUtils.putDouble(buffer.getVirtualAddress().getAddress() +
                        recordOffset + arrayValueOffset, i);
            }
            physicalVariableLengthLayout.incrementRecordNumber(buffer);
        }

        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        Catalog.getInstance().registerLayout("table.array", physicalVariableLengthLayout);
        Catalog.getInstance().registerBuffer("table.array", buffer);

        Scan scan = new Scan("table.array");
        PythonOperator selection = new PythonOperator(new PythonScalarUDF("" +
                //"import site\n" +
                //"import numpy as np\n" +
                "def udf(rec):\n" +
                "\tx=np.arange(1000)\n" +
                "\treturn x.sum()\n" +
                "\nlambda rec,ctx: udf(rec)"));
        scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);

        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan(sink));
        Thread.sleep(1000);
        for (int i = 0; i < 10000; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }

    @LuthRecord(name = "arrayValue")
    public class LinArrayValue implements Record {

        @MaxLength(length = 100)
        public int[] x;

        @MaxLength(length = 100)
        public int[] y;
    }

    @Test
    public void executeRxUDF() throws IOException, InterruptedException, SchemaExtractionException {
        TCPHImporter.importTCPH("/tpch/");


        Scan scan = new Scan("table.orders");
        PythonOperator selection = new PythonOperator(new PythonScalarUDF("" +
                //"import site\n" +
                "import re\n" +
                "pattern = re.compile('NOT SPECIFIED*')\n" +
                "def udf(rec,ctx):\n" +
                "\trs = re.sub(pattern,'none', rec.o_orderpriority.asString())\n" +
                "\trec.o_orderpriority = rs\n" +
                "\treturn rec\n" +
                "\nlambda rec,ctx: udf(rec,ctx)"));
        scan.addChild(selection);
        Projection projection = new Projection(
                new FieldReference("o_orderpriority", Text.class));
        scan.addChild(projection);
        Sink sink = new Sink.MemorySink();
        projection.addChild(sink);

        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan(sink));
        Thread.sleep(1000);
        for (int i = 0; i < 10000; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }


    @Test
    public void executePandasUDF() throws IOException, InterruptedException, SchemaExtractionException {
        AirlineImporter.importAirlineData("/combined.csv");


        PythonOperator selection = new PythonOperator(new PythonPandasUDF("" +
                "def udf():\n" +
                "\tflightsDf=pd.read(\"table.airline\")\n" +
                "\tflightsDf=flightsDf.filter(flightsDf.Cancelled.eq(False))\n" +
                "\tflightsDf=flightsDf.filter(flightsDf.DepDelay.ge(10))\n" +
                "\tflightsDf=flightsDf.filter(pd.bor(flightsDf.IATA_CODE_Reporting_Airline.eq('AA'), flightsDf.IATA_CODE_Reporting_Airline.eq('HA')))\n" +
                "\tflightsDf.avgDelay=flightsDf.DepDelay.add(flightsDf.ArrDelay).div(2)\n" +
                "\tflightsDf.loc(flightsDf.avgDelay.ge(30), 'delay').assign(\"High\")\n" +
                "\tflightsDf.loc(flightsDf.avgDelay.lt(30), 'delay').assign(\"Medium\")\n" +
                "\tflightsDf.loc(flightsDf.avgDelay.lt(20), 'delay').assign(\"Low\")\n" +
                "\treturn flightsDf.project(\"avgDelay\", \"delay\")\n" +
                "lambda: udf()"));

        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);

        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan(sink));
        Thread.sleep(1000);
        for (int i = 0; i < 10000; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }

    @Test
    public void executePandasCrimeIndexUDF() throws IOException, InterruptedException, SchemaExtractionException {
        CrimeIndexImporter.importCrimeData("/home/pgrulich/projects/luth-org/us_cities_crime_data.csv");


        PythonOperator selection = new PythonOperator(new PythonPandasUDF("" +
                "def udf():\n" +
                "\tdata=pd.read(\"table.crime\")\n" +
                "\tdata_big_cities = data.filter(data.total_population.ge(500000.0))\n" +
                "\tdata_big_cities.crime_index = data_big_cities.total_population." +
                "add(data_big_cities.total_adult_population.mul(2.0))" +
                ".add(data_big_cities.number_of_robberies.mul(-2000.0))\n" +
                "\treturn data_big_cities.project(\"crime_index\")\n" +
                "lambda: udf()"));

        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);

        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan(sink));
        Thread.sleep(1000);
        for (int i = 0; i < 10000; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }

    @LuthRecord(name = "dateValue")
    public class DateValue implements Record {

        public DateValue(Date value) {
            this.ts = value;
        }

        public Date ts;
    }

    @Test
    public void executeArrow() throws IOException, InterruptedException, SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        int size = 100_000;
        Schema schema = RecordUtil.createSchema(DateValue.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long bufferSize = ((((long) physicalSchema.getFixedRecordSize()) * size) + 1024);
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(bufferSize));
        PhysicalLayoutFactory.ColumnLayoutFactory fac = new PhysicalLayoutFactory.ColumnLayoutFactory();
        PhysicalLayout physicalVariableLengthLayout = fac.create(physicalSchema, buffer.getSize().getBytes());
        physicalVariableLengthLayout.initBuffer(buffer);
        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        Random random = new Random(24);
        for (long records = 0; records < size; records++) {
            long recordOffset = physicalVariableLengthLayout.getFieldBufferOffset(records, 0).getAddress();
            int year = random.nextInt(40) + 1990;
            Date d = new Date(year + "-01-01");
            UnsafeUtils.putInt(buffer.getVirtualAddress().getAddress() +
                    recordOffset, d.unixTs);

            physicalVariableLengthLayout.incrementRecordNumber(buffer);
        }

        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        Catalog.getInstance().registerLayout("table.array", physicalVariableLengthLayout);
        Catalog.getInstance().registerBuffer("table.array", buffer);

        Scan scan = new Scan("table.array");
        PythonOperator selection = new PythonOperator(new PythonScalarUDF("" +
                //"import site\n" +
                "import arrow as ar\n" +
                "def udf(rec,ctx):\n" +
                "\tif(rec.ts.year() > 1990 and rec.ts.year() < 1992):\n" +
                "\t\tpacfic = ar.get(rec.ts.asTs())\n" +
                "\t\tpacfic = pacfic.to('US/Pacific')\n" +
                "\t\trs = pacfic.year\n" +
                "\t\treturn rs\n" +
                "\treturn 0\n" +
                "\nlambda rec,ctx: udf(rec,ctx)"));
        scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);

        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan(sink));
        Thread.sleep(1000);
        for (int i = 0; i < 10000; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }

    @LuthRecord(name = "dateValue")
    public class Trip implements Record {

        public Trip(int start_lon, int end_lon, int start_lat, int end_lat) {
            this.start_lon = start_lon;
            this.end_lon = end_lon;
            this.start_lat = start_lat;
            this.end_lat = end_lat;
        }

        public int start_lat;
        public int start_lon;
        public int end_lat;
        public int end_lon;

    }

    @Test
    public void executeDistance() throws IOException, InterruptedException, SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        int size = 100000;
        Schema schema = RecordUtil.createSchema(Trip.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long bufferSize = ((((long) physicalSchema.getFixedRecordSize()) * size) + 1024);
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(bufferSize));
        PhysicalLayoutFactory.ColumnLayoutFactory fac = new PhysicalLayoutFactory.ColumnLayoutFactory();
        PhysicalLayout physicalVariableLengthLayout = fac.create(physicalSchema, buffer.getSize().getBytes());
        physicalVariableLengthLayout.initBuffer(buffer);
        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        Random random = new Random(24);
        for (long records = 0; records < size; records++) {
            int startLat = random.nextInt(360) - 180;
            int startLon = random.nextInt(180) - 90;
            int endLat = random.nextInt(360) - 180;
            int endLon = random.nextInt(180) - 90;

            long recordOffset = physicalVariableLengthLayout.getFieldBufferOffset(records, 0).getAddress();
            UnsafeUtils.putInt(buffer.getVirtualAddress().getAddress() +
                    recordOffset, startLat);

            recordOffset = physicalVariableLengthLayout.getFieldBufferOffset(records, 1).getAddress();
            UnsafeUtils.putInt(buffer.getVirtualAddress().getAddress() +
                    recordOffset, startLon);

            recordOffset = physicalVariableLengthLayout.getFieldBufferOffset(records, 2).getAddress();
            UnsafeUtils.putInt(buffer.getVirtualAddress().getAddress() +
                    recordOffset, endLat);

            recordOffset = physicalVariableLengthLayout.getFieldBufferOffset(records, 3).getAddress();
            UnsafeUtils.putInt(buffer.getVirtualAddress().getAddress() +
                    recordOffset, endLon);
            physicalVariableLengthLayout.incrementRecordNumber(buffer);
        }

        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        Catalog.getInstance().registerLayout("table.array", physicalVariableLengthLayout);
        Catalog.getInstance().registerBuffer("table.array", buffer);

        Scan scan = new Scan("table.array");
        PythonOperator selection = new PythonOperator(new PythonScalarUDF("" +
                //"import site\n" +
                "from haversine import haversine, Unit\n\n" +
                "def udf(rec,ctx):\n" +
                "\tstart = (rec.start_lat, rec.start_lon)\n" +
                "\tend = (rec.end_lat, rec.end_lon)\n" +
                "\tdistance = haversine(start, end)\n" +
                "\treturn distance\n" +
                "\nlambda rec,ctx: udf(rec)"));
        scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);

        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan(sink));
        Thread.sleep(1000);
        for (int i = 0; i < 10000; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }


    @Test
    public void executeNumpyUDFLinearAlgebra() throws IOException, InterruptedException, SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        int size = 10000;
        Schema schema = RecordUtil.createSchema(LinArrayValue.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long bufferSize = ((((long) physicalSchema.getFixedRecordSize()) * size) + 1024);
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(bufferSize));
        PhysicalLayoutFactory.ColumnLayoutFactory fac = new PhysicalLayoutFactory.ColumnLayoutFactory();
        PhysicalLayout physicalVariableLengthLayout = fac.create(physicalSchema, buffer.getSize().getBytes());
        physicalVariableLengthLayout.initBuffer(buffer);
        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");

        for (long records = 0; records < size; records++) {
            long field1Offset = physicalVariableLengthLayout.getFieldBufferOffset(records, 0).getAddress();
            long field2Offset = physicalVariableLengthLayout.getFieldBufferOffset(records, 1).getAddress();
            for (int i = 0; i < 100; i++) {
                int arrayValueOffset = i * 4;
                UnsafeUtils.putInt(buffer.getVirtualAddress().getAddress() +
                        field1Offset + arrayValueOffset, i + 100);
                UnsafeUtils.putInt(buffer.getVirtualAddress().getAddress() +
                        field2Offset + arrayValueOffset, i + 200);
            }
            physicalVariableLengthLayout.incrementRecordNumber(buffer);
        }

        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        Catalog.getInstance().registerLayout("table.array", physicalVariableLengthLayout);
        Catalog.getInstance().registerBuffer("table.array", buffer);

        Scan scan = new Scan("table.array");
        PythonOperator selection = new PythonOperator(new PythonScalarUDF("" +

                // "import numpy as np\n" +
                "def udf(rec,ctx):\n" +
                "\tx=rec.x.asNp()\n" +
                "\ty=rec.y.asNp()\n" +
                "\tm = (len(x) * np.sum(np.dot(x,y)) - np.sum(x) * np.sum(y)) / (len(x) * np.sum(np.dot(x,x)) - np.sum(x) * np.sum(x))\n" +
                "\tb = (np.sum(y) - m * np.sum(x)) / len(x)\n" +
                "\treturn m * 42 + b\n" +
                //"\treturn np.sum(y)\n" +
                "\nlambda rec,ctx: udf(rec)"));
        scan.addChild(selection);
        Sink sink = new Sink.PrintSink();
        selection.addChild(sink);

        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan(sink));
        Thread.sleep(1000);
        for (int i = 0; i < 10000; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }

    @LuthRecord(name = "arrayValue")
    public class BlackScholesValue implements Record {
        // random prices between 1 and 101
        @MaxLength(length = 100)
        public double[] price;

        // random prices between 0 and 101
        @MaxLength(length = 100)
        public double[] strike;

        // random maturity between 0 and 4
        @MaxLength(length = 100)
        public double[] t;

        // random rate between 0 and 1
        @MaxLength(length = 100)
        public double[] rate;

        // random volatility between 0 and 1
        @MaxLength(length = 100)
        public double[] vol;
    }

    @Test
    public void executeNumpyUDFBlackscholes() throws IOException, InterruptedException, SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        int size = 10000;
        Schema schema = RecordUtil.createSchema(BlackScholesValue.class);
        PhysicalSchema physicalSchema = new PhysicalSchema.Builder(schema).build();
        long bufferSize = ((((long) physicalSchema.getFixedRecordSize()) * size) + 1024);
        Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(bufferSize));
        PhysicalLayoutFactory.ColumnLayoutFactory fac = new PhysicalLayoutFactory.ColumnLayoutFactory();
        PhysicalLayout physicalVariableLengthLayout = fac.create(physicalSchema, buffer.getSize().getBytes());
        physicalVariableLengthLayout.initBuffer(buffer);
        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        Random random = new Random(2592);
        for (long records = 0; records < size; records++) {
            // price random prices between 1 and 101
            long priceArrayOffset = physicalVariableLengthLayout.getFieldBufferOffset(records, 0).getAddress();
            for (int i = 0; i < 100; i++) {
                double price = random.nextDouble() * 100.0;
                UnsafeUtils.putDouble(buffer.getVirtualAddress().getAddress() + priceArrayOffset + i * 8, price);
            }

            // price random prices between 1 and 101
            long strikeArrayOffset = physicalVariableLengthLayout.getFieldBufferOffset(records, 1).getAddress();
            for (int i = 0; i < 100; i++) {
                double strike = random.nextDouble() * 100.0;
                UnsafeUtils.putDouble(buffer.getVirtualAddress().getAddress() + strikeArrayOffset + i * 8, strike);
            }

            //  random maturity between 0 and 4
            long maturityArrayOffset = physicalVariableLengthLayout.getFieldBufferOffset(records, 2).getAddress();
            for (int i = 0; i < 100; i++) {
                double t = 1.0 + random.nextDouble() * 6.0;
                UnsafeUtils.putDouble(buffer.getVirtualAddress().getAddress() + maturityArrayOffset + i * 8, t);
            }

            // random rate between 0 and 1
            long rateArrayOffset = physicalVariableLengthLayout.getFieldBufferOffset(records, 3).getAddress();
            for (int i = 0; i < 100; i++) {
                double rate = 0.01 + random.nextDouble();
                UnsafeUtils.putDouble(buffer.getVirtualAddress().getAddress() + rateArrayOffset + i * 8, rate);
            }

            // random volatility between 0 and 1
            long volatilityArrayOffset = physicalVariableLengthLayout.getFieldBufferOffset(records, 4).getAddress();
            for (int i = 0; i < 100; i++) {
                double vol = 0.01 + random.nextDouble();
                UnsafeUtils.putDouble(buffer.getVirtualAddress().getAddress() + volatilityArrayOffset + i * 8, vol);
            }
            physicalVariableLengthLayout.incrementRecordNumber(buffer);
        }

        System.out.println("Buffer has " + physicalVariableLengthLayout.getNumberOfRecordsInBuffer(buffer) + " records");
        Catalog.getInstance().registerLayout("table.array", physicalVariableLengthLayout);
        Catalog.getInstance().registerBuffer("table.array", buffer);

        Scan scan = new Scan("table.array");
        PythonOperator selection = new PythonOperator(new PythonScalarUDF("" +
                // "import site\n" +
                // "import numpy as np\n" +
                "def udf(rec):\n" +
                "\trate=rec.rate.asNp()\n" +
                "\tvol=rec.vol.asNp()\n" +
                "\tt=rec.t.asNp()\n" +
                "\tprice=rec.price.asNp()\n" +
                "\tstrike=rec.strike.asNp()\n" +
                "\tinvsqrt2 = 0.707\n" +
                "\tc05 = 3.0\n" +
                "\tc10 = 1.5\n" +
                //rsig = rate + (vol * vol) * c05
                "\tvol_col = np.dot(vol,vol)\n" +
                "\tvol_c05 = np.dot(vol_col, c05)\n" +
                "\trsig = np.add(rate,vol_c05)\n" +
                "\tvol_sqrt = np.dot(vol,np.sqrt(t))\n" +
                "\tprice_strike = np.div(price,strike)\n" +
                "\tlog = np.log(price_strike)\n" +
                "\trsig_t = np.dot(rsig,t)\n" +
                "\tlog_rsig = np.add(log,rsig_t)\n" +
                "\td1 = np.div(log_rsig,vol_sqrt)\n" +
                "\td2 = np.sub(d1,vol_sqrt)\n" +
                "\td1_invsqrt2 = np.dot(d1, invsqrt2)\n" +
                "\terfD1 = np.erf(d1_invsqrt2)\n" +
                "\td1 = np.add(np.dot(erfD1, c05), c05)\n" +
                "\td2_invsqrt2 = np.dot(d2, invsqrt2)\n" +
                "\terfD2= np.erf(d2_invsqrt2)\n" +
                "\td2 = np.add(np.dot(erfD2, c05), c05)\n" +
                "\te_rt = np.exp(np.dot(np.sub(0.0,rate),t))\n" +
                "\tcall = np.sub(np.dot(price,d1), np.dot(e_rt, np.dot(strike ,d2)))\n" +
                // e_rt * strike * (c10 - d2) - price * (c10 - d1)
                "\tc10_d2 = np.sub(c10, d2)\n" +
                "\tc10_d1 = np.sub(c10, d1)\n" +
                "\tert_strike = np.dot(e_rt, strike)\n" +
                "\tert_strike = np.dot(ert_strike, c10_d2)\n" +
                "\tprice_c10 = np.dot(price, c10_d1)\n" +
                "\tput = np.sub(ert_strike, price_c10)\n" +
                "\treturn np.sum(np.add(call,put))\n" +
                "\nlambda rec,ctx: udf(rec)"));
        scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);

        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan(sink));
        Thread.sleep(1000);
        for (int i = 0; i < 10000; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }


    @Test
    public void executeNumpyUDF() throws IOException, InterruptedException, SchemaExtractionException {

        Context context = Context.newBuilder("python", "llvm")
                .option("python.VerboseFlag", "true")
                //.option("python.ForceImportSite", "true")
                .option("python.SysPrefix", "/home/pgrulich/projects/luth-org/luth/graalpython_venv")
                .option("python.SysBasePrefix", "/home/pgrulich/tools/java/graalvm-ce-java8-20.3.0/jre/languages/python")
                .option("python.StdLibHome", "/home/pgrulich/tools/java/graalvm-ce-java8-20.3.0/jre/languages/python/lib-python/3")
                .option("python.CAPI", "/home/pgrulich/tools/java/graalvm-ce-java8-20.3.0/jre/languages/python/lib-graalpython")


                .allowAllAccess(true).build();
        Source s = Source.newBuilder("python",
                //"import numpy as np\n" +
                // "import time\n" +
                "def udf(rec, ctx):\n" +
                        "\treturn np.int32(10).item()\n" +
                        "def benchmark():\n" +
                        "\tstart = time.time()\n" +
                        "\tsum = 0\n" +
                        "\tfor n in range(1, 10000):\n" +
                        "\t\tsum = sum + udf(n, n)\n" +
                        "\tend = time.time()\n" +
                        "\tprint(end - start)\n" +
                        "\treturn sum\n" +
                        "lambda rec,ctx: benchmark()", "testPlan").build();
        Value pipeline = context.eval(s);
        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();
            System.out.println(pipeline.execute(0, 0));
            long end = System.currentTimeMillis();
            System.out.println(end - start);
        }

    }

    @Test
    public void executePythonReturnUDF() throws IOException, InterruptedException, SchemaExtractionException {
        TCPHImporter.importTCPH("/tpch/");
        LogicalOperator scan = pythonReturnUDF();
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan((Sink) scan));
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }

    @Test
    public void executePythonCallbackUDF() throws IOException, InterruptedException, SchemaExtractionException {

        LogicalOperator scan = pythonCallbackUDF();
        Thread.sleep(1000);
        Value executableQuery = submitQuery(new LogicalQueryPlan((Scan) scan));
        Thread.sleep(1000);
        for (int i = 0; i < 100; i++) {
            Value time = executableQuery.execute(new BufferArgument(buffer, outputBufferManager));
            System.out.println("Execution Time:" + time);
            //outputBufferManager.releaseAll();
            if (time.asLong() == 0)
                return;
            Thread.sleep(100);
        }
        Assert.fail();
    }


    static Value submitQuery(LogicalQueryPlan plan) {

        try {
            Context context = Context.newBuilder("luth", "js", "python")
                    //.environment("VIRTUAL_ENV", "/home/pgrulich/tools/java/graalvm-ce-java8-20.3.0/bin/my_new_venv")
                    .option("engine.IterativePartialEscape", "true")
                    .option("engine.MultiTier", "false")
                    .option("engine.TraceTransferToInterpreter", "true")
                    .option("python.VerboseFlag", "true")
                    //.option("python.PythonOptimizeFlag", "true")

                    //     .option("engine.UseExceptionProbability", "false")
                    // .option("python.ForceImportSite", "true")
                    //.option("python.SysPrefix", "./graalpython_venv")
                    //.option("python.SysBasePrefix", "/graalvm-ce-java8-20.3.0/jre/languages/python")
                    //.option("python.StdLibHome", "/graalvm-ce-java8-20.3.0/jre/languages/python/lib-python/3")
                    //.option("python.CAPI", "/graalvm-ce-java8-20.3.0/jre/languages/python/lib-graalpython")
                    //.option("engine.Inlining", "false")
                    //.option("engine.CompileImmediately", "true")
                    //.option("engine.Compilation", "false")
                    //.option("engine.BackgroundCompilation", "false")
                    .option("engine.TraceCompilation", "true")
                    // .option("engine.Profiling", "false")

                    .option("python.NoAsyncActions", "true")
                    //.option("engine.TraceInlining", "true")
                    //.option("engine.TraceInliningDetails", "true")
                    // default 150000
                    .option("engine.MaximumInlineNodeCount", "800000")
                    // defualt 30_000
                    .option("engine.InliningExpansionBudget", "200000")
                    // defualt 30_000
                    .option("engine.InliningInliningBudget", "200000")
                    // default 400000
                    .option("engine.MaximumGraalNodeCount", "800000")
                    //.option("engine.CompilationStatistics", "true")
                    //.option("engine.CompilationStatisticDetails", "true")
                    //.option("engine.TraceCompilationDetails", "true")
                    .option("engine.CompilationExceptionsArePrinted", "true")
                    .allowAllAccess(true).build();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(plan);
            oos.flush();
            oos.close();
            Source s = Source.newBuilder("luth", ByteSequence.create(baos.toByteArray()), "testPlan").build();
            Value pipeline = context.eval(s);
            return pipeline;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


}
