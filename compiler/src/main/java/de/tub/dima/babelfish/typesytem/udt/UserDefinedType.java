package de.tub.dima.babelfish.typesytem.udt;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface UserDefinedType {
    String name() default "";
}
