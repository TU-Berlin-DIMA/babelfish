package de.tub.dima.babelfish.typesytem.variableLengthType;


import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface MaxLength {
    int length();
}
