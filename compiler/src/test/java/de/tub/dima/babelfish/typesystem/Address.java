package de.tub.dima.babelfish.typesystem;

import de.tub.dima.babelfish.typesytem.record.*;
import de.tub.dima.babelfish.typesytem.udt.Point;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_8;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

@LuthRecord
public final class Address implements Record {
    Text city;
    Text street;
    Int_8 number;
    Point location;
}
