package de.tub.dima.babelfish.typesytem.record;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface LuthRecord {
    String name() default "";
}
