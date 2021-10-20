package de.tub.dima.babelfish.typesytem.valueTypes.number.numeric;

import de.tub.dima.babelfish.storage.UnsafeUtils;

public class CSVSourceNumeric extends Numeric {

    private final long startPosition;
    private final long endPosition;

    private long cachedValue;
    public boolean cached;

    public CSVSourceNumeric(long startPosition, long endPosition, int precision) {
        super(precision);
        this.startPosition = startPosition;
        this.endPosition = endPosition;
    }

    @Override
    public long getValue() {
        long resultValue = 0;

        if (precision == 0) {
            for (long position = startPosition;
                 position < endPosition;
                 position++) {
                byte value = UnsafeUtils.getByte(position);
                int charValue = value - '0';
                resultValue = resultValue * 10 + charValue;
            }
        } else {
            for (long position = startPosition;
                 position < endPosition - precision - 1;
                 position++) {
                byte value = UnsafeUtils.getByte(position);
                int charValue = value - '0';
                resultValue = resultValue * 10 + charValue;
            }

            for (long position = endPosition - precision;
                 position < endPosition;
                 position++) {
                byte value = UnsafeUtils.getByte(position);
                int charValue = value - '0';
                resultValue = resultValue * 10 + charValue;
            }
        }

        cachedValue = resultValue;
        cached = true;
        return resultValue;
    }

    public long getCachedValue() {
        return cachedValue;
    }
}
