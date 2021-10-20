package de.tub.dima.babelfish.benchmark.externalLibs;

import de.tub.dima.babelfish.benchmark.parser.AirlineImporter;
import de.tub.dima.babelfish.benchmark.parser.CrimeIndexImporter;
import de.tub.dima.babelfish.benchmark.parser.TCPHImporter;
import de.tub.dima.babelfish.ir.lqp.LogicalOperator;
import de.tub.dima.babelfish.ir.lqp.Scan;
import de.tub.dima.babelfish.ir.lqp.Sink;
import de.tub.dima.babelfish.ir.lqp.relational.Projection;
import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonOperator;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonPandasUDF;
import de.tub.dima.babelfish.ir.lqp.udf.python.PythonScalarUDF;
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

import java.io.IOException;
import java.util.Random;

/**
 * Query implementation to benchmark performance of UDFs that embed 3rd-party libraries.
 */
public class Queries {

    public static Sink getReQueries() throws IOException, SchemaExtractionException {
        TCPHImporter.importTCPH("/tpch/");
        Scan scan = new Scan("table.orders");
        // Define a map UDF that embeds the re library to compute regular expressions.
        PythonOperator map = new PythonOperator(new PythonScalarUDF("" +
                "import re\n" +
                "pattern = re.compile('NOT SPECIFIED*')\n" +
                "def udf(rec,ctx):\n" +
                "\trs = re.sub(pattern,'none', rec.o_orderpriority.asString())\n" +
                "\trec.o_orderpriority = rs\n" +
                "\treturn rec\n" +
                "\nlambda rec,ctx: udf(rec,ctx)"));
        scan.addChild(map);
        Projection projection = new Projection(
                new FieldReference("o_orderpriority", Text.class));
        scan.addChild(projection);
        Sink sink = new Sink.MemorySink();
        projection.addChild(sink);
        return sink;
    }

    @LuthRecord(name = "arrayValue")
    public class LinArrayValue implements Record {

        @MaxLength(length = 100)
        public int[] x;

        @MaxLength(length = 100)
        public int[] y;
    }

    public static LogicalOperator getLinearRegression() throws SchemaExtractionException {
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
        // Defines a map UDF that operates on BF array fields with numpy to compute a linear regression.
        PythonOperator map = new PythonOperator(new PythonScalarUDF("" +
                "def udf(rec,ctx):\n" +
                "\tx=rec.x.asNp()\n" +
                "\ty=rec.y.asNp()\n" +
                "\tm = (len(x) * np.sum(x * y) - np.sum(x) * np.sum(y)) / (len(x) * np.sum(x * x) - np.sum(x) * np.sum(x))\n" +
                "\tb = (np.sum(y) - m * np.sum(x)) / len(x)\n" +
                "\treturn m * 42 + b\n" +
                "\nlambda rec,ctx: udf(rec)"));
        scan.addChild(map);
        Sink sink = new Sink.MemorySink();
        map.addChild(sink);
        return sink;
    }

    @LuthRecord(name = "dateValue")
    public class DateValue implements Record {

        public DateValue(Date value) {
            this.ts = value;
        }

        public Date ts;
    }

    public static Sink getArrowQuery() throws IOException, SchemaExtractionException {
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
        // defines a map udf that embeds the arrow library to convert the timezone of a value
        PythonOperator map = new PythonOperator(new PythonScalarUDF("" +
                "import site\n" +
                "import arrow as ar\n" +
                "def udf(rec,ctx):\n" +
                "\tif(rec.ts.year() > 1990 and rec.ts.year() < 1992):\n" +
                "\t\tpacfic = ar.get(rec.ts.asTs())\n" +
                "\t\tpacfic = pacfic.to('US/Pacific')\n" +
                "\t\trs = pacfic.year\n" +
                "\t\treturn rs\n" +
                "\treturn 0\n" +
                "\nlambda rec,ctx: udf(rec,ctx)"));
        scan.addChild(map);
        Sink sink = new Sink.MemorySink();
        map.addChild(sink);
        return sink;
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

    public static Sink getDistanceFunction() throws SchemaExtractionException {
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
        // Defines a UDF that embeds the haversine UDF to calculate the distance between two points.
        PythonOperator mapUDF = new PythonOperator(new PythonScalarUDF("" +
                "import site\n" +
                "from haversine import haversine, Unit\n\n" +
                "def udf(rec,ctx):\n" +
                "\tstart = (rec.start_lat, rec.start_lon)\n" +
                "\tend = (rec.end_lat, rec.end_lon)\n" +
                "\tdistance = haversine(start, end)\n" +
                "\treturn distance\n" +
                "\nlambda rec,ctx: udf(rec)"));
        scan.addChild(mapUDF);
        Sink sink = new Sink.MemorySink();
        mapUDF.addChild(sink);

        return sink;
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

    public static Sink getBlackScholes() throws SchemaExtractionException {

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
        // Defines a UDF that embeds numpy to calculate the Black Scholes algorithm as proposed by
        // Inspired by https://github.com/weld-project/weld/blob/v0.2.0/python/numpy/examples/blackscholes/blackscholes.py
        PythonOperator selection = new PythonOperator(new PythonScalarUDF("" +
                "def udf(rec, ctx):\n" +
                "\trate=rec.rate.asNp()\n" +
                "\tvol=rec.vol.asNp()\n" +
                "\tt=rec.t.asNp()\n" +
                "\tprice=rec.price.asNp()\n" +
                "\tstrike=rec.strike.asNp()\n" +
                "\tinvsqrt2 = 0.707\n" +
                "\tc05 = 3.0\n" +
                "\tc10 = 1.5\n" +
                "\trsig = rate + ((vol * vol) * c05)\n" +
                "\tvol_sqrt = vol * np.sqrt(t)\n" +
                "\td1 = (np.log(price / strike) + (rsig * t)) / vol_sqrt\n" +
                "\td2 = d1 - vol_sqrt\n" +
                "\td1 = c05 + c05 * np.erf(d1 * invsqrt2)\n" +
                "\td2 = c05 + c05 * np.erf(d2 * invsqrt2)\n"+
                "\te_rt = np.exp((0.0 - rate) * t)\n" +
                "\tcall = (price * d1) - (e_rt * (strike * d2))\n"+
                // e_rt * strike * (c10 - d2) - price * (c10 - d1)
                "\tput = (e_rt * (strike * (c10 - d2))) - (price * (c10 - d1))\n"+
                "\treturn np.sum(call + put)\n" +
                "\nlambda rec,ctx: udf(rec)"));
        scan.addChild(selection);
        Sink sink = new Sink.MemorySink();
        selection.addChild(sink);
        return sink;
    }

    public static Sink getCrimeIndex() throws IOException, SchemaExtractionException {

        CrimeIndexImporter.importCrimeData("/home/pgrulich/projects/luth-org/us_cities_crime_data.csv");

        // Defines a map UDF that implements the crime index calculation inspired by the following example
        // https://github.com/weld-project/weld/blob/master/examples/python/grizzly/get_population_stats_simplified_grizzly.py
        PythonOperator mapUDF = new PythonOperator(new PythonPandasUDF("" +
                "def udf():\n" +
                "\tdata=pd.read(\"table.crime\")\n" +
                "\tdata_big_cities = data[data.total_population >= 500000.0]\n" +
                "\tdata_big_cities.crime_index = data_big_cities.total_population +" +
                " (data_big_cities.total_adult_population * 2.0) + (data_big_cities.number_of_robberies * -2000.0)\n" +
                "\treturn data_big_cities[[\"crime_index\"]]\n" +
                "lambda: udf()"));

        Sink sink = new Sink.MemorySink();
        mapUDF.addChild(sink);
        return sink;
    }

    public static Sink getAirlinesEtl() throws IOException, SchemaExtractionException {
        AirlineImporter.importAirlineData("/combined.csv");
        // Defines an map UDF that implements the airline ETL workload using a single pandas UDF.
        // Compare with de/tub/dima/babelfish/benchmark/analytics/queries/ETLQuery.java
        PythonOperator mapUDF = new PythonOperator(new PythonPandasUDF("" +
                "def udf():\n" +
                "\tflightsDf=pd.read(\"table.airline\")\n" +
                "\tflightsDf=flightsDf[flightsDf.Cancelled == False]\n" +
                "\tflightsDf=flightsDf[flightsDf.DepDelay >= 10]\n" +
                "\tflightsDf=flightsDf[(flightsDf.IATA_CODE_Reporting_Airline == 'AA') | (flightsDf.IATA_CODE_Reporting_Airline == 'HA')]\n" +
                "\tflightsDf.avgDelay= (flightsDf.DepDelay + flightsDf.ArrDelay) / 2\n" +
                "\tflightsDf.loc[flightsDf.avgDelay >= 30, 'delay'] = \"High\"\n" +
                "\tflightsDf.loc[flightsDf.avgDelay < 30, 'delay'] = \"Medium\"\n" +
                "\tflightsDf.loc[flightsDf.avgDelay < 20, 'delay'] = \"Low\"\n" +
                "\treturn flightsDf[[\"avgDelay\", \"delay\"]]\n" +
                "lambda: udf()"));

        Sink sink = new Sink.MemorySink();
        mapUDF.addChild(sink);
        return sink;
    }
}
