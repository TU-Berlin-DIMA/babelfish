package de.tub.dima.babelfish.typesytem.record;

import com.google.auto.service.*;
import de.tub.dima.babelfish.typesytem.udt.*;
import de.tub.dima.babelfish.typesytem.valueTypes.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;
import de.tub.dima.babelfish.typesytem.variableLengthType.*;

import javax.annotation.processing.*;
import javax.lang.model.*;
import javax.lang.model.element.*;
import javax.lang.model.type.*;
import java.lang.*;
import java.lang.Character;
import java.lang.Float;
import java.util.*;

@SupportedAnnotationTypes(
        "de.tub.dima.babelfish.typesytem.record.LuthRecord")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@AutoService(Processor.class)
public class RecordAnnotationProcessor extends AbstractProcessor {


    @Override
    public boolean process(Set<? extends TypeElement> annotations,
                           RoundEnvironment roundEnv) {
        /*for (TypeElement annotation : annotations) {
            Set<? extends Element> annotatedElements
                    = roundEnv.getElementsAnnotatedWith(annotation);

            for (Element e : annotatedElements) {
                TypeElement typeElement = ((TypeElement) e);

                if (!AnnotationUtils.isFinal(typeElement)) {
                    this.processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, ((TypeElement) e).getQualifiedName() + " is annotated as @Record so it has to be final", e, e.getAnnotationMirrors().get(0));
                }

                for (Element ee : typeElement.getEnclosedElements()) {

                    if (ee.getKind() == ElementKind.FIELD) {

                        if (!isValidType(ee.asType())) {
                            //this.processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "Type " + ee.asType() + " of " + ee.getSimpleName() + " is not valid", e, e.getAnnotationMirrors().get(0));
                        }


                        if (AnnotationUtils.isPrivate(ee)) {
                            this.processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, ee.getSimpleName() + " must not be private", e, e.getAnnotationMirrors().get(0));
                        }
                        //if (!AnnotationUtils.isFinal(ee)) {
                        //     this.processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, ee.getSimpleName() + " has to be final", e, e.getAnnotationMirrors().get(0));
                        //}
                    }
                }
            }
        }
        return false;
        */
        return true;
    }

    private boolean isValidType(TypeMirror typeMirror) {
        DeclaredType t2 = (DeclaredType) typeMirror;
        Element e = t2.asElement();

        //  this.processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING, t2.asElement().toString());
        //    this.processingEnv.getTypeUtils().isSubtype(typeMirror, createTypeMirror(Record.class));

       // for (AnnotationMirror am : e.getAnnotationMirrors()) {
       //     if(isSame(createTypeMirror(LuthRecord.class), am.getAnnotationType().getEnclosingType()))
       //         this.processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING, am.toString());
       // }

        if (isSame(typeMirror, createTypeMirror(Text.class))) {
            return true;
        }

        if (typeMirror instanceof ArrayType) {
            typeMirror = ((ArrayType) typeMirror).getComponentType();
        }

        if (typeMirror instanceof PrimitiveType)
            return true;

        if (isUDT(typeMirror))
            return true;

        return isPrimitive(typeMirror);

    }

    private boolean isUDT(TypeMirror typeMirror) {
        return this.processingEnv.getTypeUtils().isSubtype(typeMirror, createTypeMirror(UDT.class));
    }

    private boolean isPrimitive(TypeMirror typeMirror) {
        return isSame(typeMirror, createTypeMirror(Integer.class)) ||
                isSame(typeMirror, createTypeMirror(Int_8.class)) ||
                isSame(typeMirror, createTypeMirror(Int_16.class)) ||
                isSame(typeMirror, createTypeMirror(Int_32.class)) ||
                isSame(typeMirror, createTypeMirror(Int_64.class)) ||
                isSame(typeMirror, createTypeMirror(Float_32.class)) ||
                isSame(typeMirror, createTypeMirror(Float_64.class)) ||
                isSame(typeMirror, createTypeMirror(Bool.class)) ||
                isSame(typeMirror, createTypeMirror(Byte.class)) ||
                isSame(typeMirror, createTypeMirror(Short.class)) ||
                isSame(typeMirror, createTypeMirror(Character.class)) ||
                isSame(typeMirror, createTypeMirror(Double.class)) ||
                isSame(typeMirror, createTypeMirror(Float.class)) ||
                isSame(typeMirror, createTypeMirror(Long.class));
    }

    private boolean isSame(TypeMirror typeMirror1, TypeMirror typeMirror2) {
        return this.processingEnv.getTypeUtils().isAssignable(typeMirror1, typeMirror2);
    }


    private TypeMirror createTypeMirror(Class<?> clazz) {
        return this.processingEnv.getElementUtils().getTypeElement(clazz.getCanonicalName()).asType();
    }


}
