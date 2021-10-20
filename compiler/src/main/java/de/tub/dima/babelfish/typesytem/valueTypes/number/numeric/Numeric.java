package de.tub.dima.babelfish.typesytem.valueTypes.number.numeric;


import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.library.DynamicDispatchLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.typesytem.valueTypes.ValueType;

@ExportLibrary(value = DynamicDispatchLibrary.class)
public abstract class Numeric implements ValueType {

    @ExportMessage
    public Class<?> dispatch() {
        return NumericBuiltins.class;
    }


    final int precision;

    protected Numeric(int precision) {
        this.precision = precision;
    }

    @CompilerDirectives.CompilationFinal(dimensions = 1)
    public static final int[] numericShifts = new int[]{
            1,
            10,
            100,
            1000,
            10000,
            100000,
            1000000,
            10000000,
            100000000,
            1000000000};



    public abstract long getValue();

    public final int getPrecision() {
        return precision;
    }

    protected final static float getFloatValue(long value, float divisor){
        return value / divisor;
    }

    protected final static double getDoubleValue(long value, double divisor){
        return value / divisor;
    }

    @Override
    public String toString() {
        return String.valueOf(getValue() / ((double) numericShifts[precision]));
    }
}
