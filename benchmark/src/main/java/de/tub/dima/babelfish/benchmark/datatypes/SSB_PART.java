package de.tub.dima.babelfish.benchmark.datatypes;

import de.tub.dima.babelfish.typesytem.record.LuthRecord;
import de.tub.dima.babelfish.typesytem.record.Record;
import de.tub.dima.babelfish.typesytem.variableLengthType.MaxLength;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

@LuthRecord(name = "ssb_SUPPLIER")
public class SSB_PART implements Record {
    public int p_partkey;
    @MaxLength(length = 22)
    public Text p_name;
    @MaxLength(length = 6)
    public Text p_mfgr;
    @MaxLength(length = 7)
    public Text p_category;
    @MaxLength(length = 9)
    public Text p_brand1;
    @MaxLength(length = 11)
    public Text p_color;
    @MaxLength(length = 25)
    public Text p_type;
    public int p_size;
    @MaxLength(length = 10)
    public Text p_container;

    public SSB_PART(int p_partkey, Text p_name, Text p_mfgr, Text p_category, Text p_brand1, Text p_color, Text p_type, int p_size, Text p_container) {
        this.p_partkey = p_partkey;
        this.p_name = p_name;
        this.p_mfgr = p_mfgr;
        this.p_category = p_category;
        this.p_brand1 = p_brand1;
        this.p_color = p_color;
        this.p_type = p_type;
        this.p_size = p_size;
        this.p_container = p_container;
    }
}
