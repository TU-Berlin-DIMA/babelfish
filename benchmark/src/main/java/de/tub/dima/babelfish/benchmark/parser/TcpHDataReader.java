package de.tub.dima.babelfish.benchmark.parser;

import de.tub.dima.babelfish.benchmark.datatypes.Lineitem;
import de.tub.dima.babelfish.benchmark.tcph.BufferDump;
import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Catalog;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.layout.GenericSerializer;
import de.tub.dima.babelfish.storage.layout.PhysicalColumnLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalLayout;
import de.tub.dima.babelfish.storage.layout.PhysicalSchema;
import de.tub.dima.babelfish.typesytem.record.RecordUtil;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.schema.Schema;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

public class TcpHDataReader {

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

            GenericSerializer.serialize(lineitemSchema, physicalVariableLengthLayout, buffer, null);
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
