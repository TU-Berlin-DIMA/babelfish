package de.tub.dima.babelfish.benchmark.datatypes;

import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.variableLengthType.MaxLength;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

@LuthRecord(name = "Lineitem")
public class Crime implements Record {
    @MaxLength(length = 2)
    public Text state;
    public double total_population;
    public double adult_population;
    public double number_of_robberies;

    public Crime(Text state, double total_population, double adult_population, double number_of_robberies) {
        this.state = state;
        this.total_population = total_population;
        this.adult_population = adult_population;
        this.number_of_robberies = number_of_robberies;
    }
}
