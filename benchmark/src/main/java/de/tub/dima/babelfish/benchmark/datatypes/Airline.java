package de.tub.dima.babelfish.benchmark.datatypes;

import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.variableLengthType.MaxLength;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

@LuthRecord(name = "Lineitem")
public class Airline implements Record {
    public int Year;
    public int Month;
    public int DayofMonth;
    @MaxLength(length = 17)
    public Text Reporting_Airline;
    @MaxLength(length = 2)
    public Text IATA_CODE_Reporting_Airline;
    public int OriginAirportID;
    @MaxLength(length = 6)
    public Text Origin;
    @MaxLength(length = 34)
    public Text OriginCityName;
    public int DestAirportID;
    @MaxLength(length = 4)
    public Text Dest;
    @MaxLength(length = 34)
    public Text DestCityName;
    public int DepTime;
    public int DepDelay;
    public int ArrTime;
    public int ArrDelay;
    public boolean Cancelled;


    public Airline(int year, int month, int dayofMonth, Text reporting_Airline, Text IATA_CODE_Reporting_Airline, int originAirportID, Text origin, Text originCityName, int destAirportID, Text dest, Text destCityName, int depTime, int depDelay, int arrTime, int arrDelay, boolean cancelled) {
        Year = year;
        Month = month;
        DayofMonth = dayofMonth;
        Reporting_Airline = reporting_Airline;
        this.IATA_CODE_Reporting_Airline = IATA_CODE_Reporting_Airline;
        OriginAirportID = originAirportID;
        Origin = origin;
        OriginCityName = originCityName;
        DestAirportID = destAirportID;
        Dest = dest;
        DestCityName = destCityName;
        DepTime = depTime;
        DepDelay = depDelay;
        ArrTime = arrTime;
        ArrDelay = arrDelay;
        Cancelled = cancelled;
    }

}
