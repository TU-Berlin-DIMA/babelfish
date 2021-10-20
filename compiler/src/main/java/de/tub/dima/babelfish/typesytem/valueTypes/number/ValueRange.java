package de.tub.dima.babelfish.typesytem.valueTypes.number;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface ValueRange {
    long minValue();
    long maxValue();

}
