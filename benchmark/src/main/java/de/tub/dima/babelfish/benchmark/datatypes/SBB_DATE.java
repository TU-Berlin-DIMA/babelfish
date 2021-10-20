package de.tub.dima.babelfish.benchmark.datatypes;

import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;

@LuthRecord(name = "ssb_DATE")
public class SBB_DATE implements Record {
    public int d_datekey;
    //public Text d_date;
    //public Text d_dayofweek;
    //public Text d_month;
    public int d_year;
    public int d_yearmonthnum;
    //public Text d_yearmonth;
    public int d_daynuminweek;
    public int d_daynuminmonth;
    public int d_daynuminyear;
    public int d_monthnuminyear;
    public int d_weeknuminyear;
    //public Text d_sellingseasin;
    public int d_lastdayinweekfl;
    public int d_lastdayinmonthfl;
    public int d_holidayfl;
    public int d_weekdayfl;

    public SBB_DATE(int d_datekey, int d_year, int d_yearmonthnum, int d_daynuminweek, int d_daynuminmonth, int d_daynuminyear, int d_monthnuminyear, int d_weeknuminyear, int d_lastdayinweekfl, int d_lastdayinmonthfl, int d_holidayfl, int d_weekdayfl) {
        this.d_datekey = d_datekey;
        this.d_year = d_year;
        this.d_yearmonthnum = d_yearmonthnum;
        this.d_daynuminweek = d_daynuminweek;
        this.d_daynuminmonth = d_daynuminmonth;
        this.d_daynuminyear = d_daynuminyear;
        this.d_monthnuminyear = d_monthnuminyear;
        this.d_weeknuminyear = d_weeknuminyear;
        this.d_lastdayinweekfl = d_lastdayinweekfl;
        this.d_lastdayinmonthfl = d_lastdayinmonthfl;
        this.d_holidayfl = d_holidayfl;
        this.d_weekdayfl = d_weekdayfl;
    }
}
