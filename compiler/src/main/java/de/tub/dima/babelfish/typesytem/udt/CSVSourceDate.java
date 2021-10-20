package de.tub.dima.babelfish.typesytem.udt;

import de.tub.dima.babelfish.storage.UnsafeUtils;

public class CSVSourceDate extends AbstractDate{

    private final long startPosition;
    private final long endPosition;

    private int cachedValue;
    public boolean cached;

    public CSVSourceDate(long startPosition, long endPosition) {
        this.startPosition = startPosition;
        this.endPosition = endPosition;
    }

    @Override
    public int getUnixTs() {
        int resultValue = 0;

        // year-mm-dd
        // year (start - start+4)
        for (long position = startPosition;
             position < startPosition + 4;
             position++) {
            byte value = UnsafeUtils.getByte(position);
            int charValue = value - '0';
            resultValue = resultValue * 10 + charValue;
        }

        // month  (start+5 - start+7)
        for (long position = startPosition +5;
             position < startPosition + 7;
             position++) {
            byte value = UnsafeUtils.getByte(position);
            int charValue = value - '0';
            resultValue = resultValue * 10 + charValue;
        }

        // month  (start+8 - start+10)
        for (long position = startPosition +8;
             position < startPosition + 10;
             position++) {
            byte value = UnsafeUtils.getByte(position);
            int charValue = value - '0';
            resultValue = resultValue * 10 + charValue;
        }
        cachedValue = resultValue;
        cached = true;
        return resultValue;
    }

    public int getCachedValue() {
        return cachedValue;
    }
}
