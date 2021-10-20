package de.tub.dima.babelfish.typesystem;

import de.tub.dima.babelfish.typesytem.record.*;
import de.tub.dima.babelfish.typesytem.schema.*;
import de.tub.dima.babelfish.typesytem.schema.field.*;
import de.tub.dima.babelfish.typesytem.tuple.*;
import de.tub.dima.babelfish.typesytem.udt.Point;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_16;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_8;
import de.tub.dima.babelfish.typesytem.variableLengthType.Array.BFArray;
import org.junit.*;

import java.lang.reflect.*;

public class SchemaTest {

    @LuthRecord
    public final class R1 implements Record {
        public BFArray intarray;
    }

    public void map(Tuple3<Int_8, Int_16, Int_32> t){

    }

    @Test
    public void RecordToSchemaTest() throws SchemaExtractionException {

        ParameterizedType paramType = (ParameterizedType) SchemaTest.class.getDeclaredMethods()[1].getGenericParameterTypes()[0];
        Class<Record> param = (Class<Record>) SchemaTest.class.getDeclaredMethods()[1].getParameterTypes()[0];

        //Type typeArgs = TypeResolver.reify(R1.class.getFields()[0].getGenericType(), R1.class);

      //  ParameterizedType paramType = (ParameterizedType) typeArgs;

        Schema s2 = SchemaBuilder
                .createBuilder()
                .addField(new UDTField("point", Point.class, 0, true, null))
                .build();


        Schema s = RecordUtil.createSchema(param, paramType);

        System.out.println(s);
    }

    @Test
    public void UDTToSchemaTest() throws SchemaExtractionException {




        Schema s = RecordUtil.createSchemaFromUDT(Point.class);

        System.out.println(s);
    }
}
