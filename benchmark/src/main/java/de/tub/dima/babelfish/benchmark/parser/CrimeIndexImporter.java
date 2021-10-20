package de.tub.dima.babelfish.benchmark.parser;

import de.tub.dima.babelfish.benchmark.datatypes.Crime;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.layout.PhysicalLayoutFactory;
import de.tub.dima.babelfish.typesytem.record.SchemaExtractionException;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;

import java.io.IOException;
import java.util.List;

import static de.tub.dima.babelfish.benchmark.parser.SSBImporter.importTable;

public class CrimeIndexImporter {

    public static void importCrimeData(String path) throws IOException, SchemaExtractionException {
        BufferManager bufferManager = new BufferManager();
        PhysicalLayoutFactory factory = new PhysicalLayoutFactory.ColumnLayoutFactory();
        // import lineorder.tbl
        importTable(bufferManager, path, "table.crime", Crime.class, (List<String> fields) -> {

            double total_population = fields.get(2).isEmpty() ? 0 : Double.parseDouble(fields.get(2));
            double adult_population = fields.get(3).isEmpty() ? 0 : Double.parseDouble(fields.get(3));
            double number_of_robberies = fields.get(4).isEmpty() ? 0 : Double.parseDouble(fields.get(4));
            return new Crime(
                    new StringText(fields.get(1)),
                    total_population,
                    adult_population,
                    number_of_robberies
            );
        }, ",", true, factory);
    }


}
