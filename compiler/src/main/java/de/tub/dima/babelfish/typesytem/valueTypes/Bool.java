package de.tub.dima.babelfish.typesytem.valueTypes;


public class Bool implements ValueType {

    private final boolean value;

    public Bool(boolean value) {
        this.value = value;
    }

    public boolean getValue(){
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Bool){
            return ((Bool) obj).value == value;
        }
        return false;
    }

}
