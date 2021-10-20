package de.tub.dima.babelfish.typesytem.record;

import de.tub.dima.babelfish.typesytem.*;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public abstract class DynamicRecord {

    public void setValue(String string, BFType value){}
    public void setValue(String string, String value){
        setValue(string, new StringText(value));
    }
    public <T extends BFType> T getValue(String string){return null;}
    public String getString(String string){
        Text text = getValue(string);
        return text.toString();
    }
}
