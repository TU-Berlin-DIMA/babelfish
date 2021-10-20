package de.tub.dima.babelfish.benchmark.parser;

import de.tub.dima.babelfish.benchmark.datatypes.Airline;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.layout.PhysicalLayoutFactory;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;

import java.io.IOException;
import java.util.List;

import static de.tub.dima.babelfish.benchmark.parser.SSBImporter.importTable;

public class AirlineImporter {

    public static void importAirlineData(String path) throws IOException, SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        PhysicalLayoutFactory factory = new PhysicalLayoutFactory.ColumnLayoutFactory();
        // import lineorder.tbl
        importTable(bufferManager, path, "table.airline", Airline.class, (List<String> fields) -> {
            int DepTime = fields.get(12).isEmpty() ? 0 : Double.valueOf(fields.get(12)).intValue();
            int ArrTime = fields.get(14).isEmpty() ? 0 : Double.valueOf(fields.get(14)).intValue();
            int DepDelay = fields.get(13).isEmpty() ? 0 : Double.valueOf(fields.get(13)).intValue();
            int ArrDelay = fields.get(15).isEmpty() ? 0 : Double.valueOf(fields.get(15)).intValue();
            return new Airline(
                    // Year
                    Integer.valueOf(fields.get(1)),
                    // Month
                    Integer.valueOf(fields.get(2)),
                    // DayofMonth
                    Integer.valueOf(fields.get(3)),
                    // Reporting_Airline
                    new StringText(fields.get(4)),
                    // IATA_CODE_Reporting_Airline
                    new StringText(fields.get(5)),
                    // OriginAirportID
                    Integer.valueOf(fields.get(6)),
                    // Origin
                    new StringText(fields.get(7)),
                    // OriginCityName
                    new StringText(fields.get(8)),
                    // DestAirportID
                    Integer.valueOf(fields.get(9)),
                    // Dest
                    new StringText(fields.get(10)),
                    // OriginDestCityNameCityName
                    new StringText(fields.get(11)),
                    // DepTime
                    DepTime,
                    // DepDelay
                    DepDelay,
                    // ArrTime
                    ArrTime,
                    // ArrDelay
                    ArrDelay,
                    // Cancelled
                    Double.valueOf(fields.get(16)).intValue() == 1
            );
        }, "\\|", true, factory);
    }


}
