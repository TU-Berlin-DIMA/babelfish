package de.tub.dima.babelfish.typesytem;

import javax.lang.model.element.*;

public class AnnotationUtils {

    public static boolean isFinal(Element typeElement) {
        return typeElement.getModifiers().contains(Modifier.FINAL);
    }

    public static boolean isPrivate(Element typeElement) {
        return typeElement.getModifiers().contains(Modifier.PRIVATE);
    }
}
