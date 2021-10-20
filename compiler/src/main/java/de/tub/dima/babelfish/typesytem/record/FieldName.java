package de.tub.dima.babelfish.typesytem.record;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface FieldName {
    String name();
}
