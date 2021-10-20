package de.tub.dima.babelfish.typesytem.udt;

import java.text.*;

public class Date extends AbstractDate implements Comparable<Date> {

    public int unixTs;

    public static Integer parse(String timeString) {
        int resultValue = 0;
        for (int position = 0; position < 4; position++) {
            byte value = (byte) timeString.charAt(position);
            int charValue = value - '0';
            resultValue = resultValue * 10 + charValue;
        }

        // month  (start+5 - start+7)
        for (int position = 5; position < 7; position++) {
            byte value = (byte) timeString.charAt(position);
            int charValue = value - '0';
            resultValue = resultValue * 10 + charValue;
        }

        // month  (start+8 - start+10)
        for (int position = 8; position < 10; position++) {
            byte value = (byte) timeString.charAt(position);
            int charValue = value - '0';
            resultValue = resultValue * 10 + charValue;
        }

        return (int) resultValue;
    }

    public Date(int unixTs) {
        this.unixTs = unixTs;
    }

    public Date(String timeString) {
        this.unixTs = parse(timeString);
    }

    public int getUnixTs() {
        return unixTs;
    }

    @Override
    public int compareTo(Date o) {
        return (int) (this.unixTs - o.unixTs);
    }

    public boolean before(Date o) {
        return this.unixTs < o.unixTs;
    }


    public boolean before(String other) {
        return before(new Date(other));
    }

    public boolean after(Date o) {
        return this.unixTs > o.unixTs;
    }

    public boolean after(String other) {
        return after(new Date(other));
    }

    @Override
    public String toString() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-m-d");
        java.util.Date value = new java.util.Date(((long) unixTs) * 1000);
        return dateFormat.format(value);
    }

}
