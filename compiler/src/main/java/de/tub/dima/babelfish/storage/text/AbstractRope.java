package de.tub.dima.babelfish.storage.text;

import com.oracle.truffle.api.library.DynamicDispatchLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.buildins.AbstractRopeBuiltins;
import de.tub.dima.babelfish.storage.text.leaf.ConstantRope;
import de.tub.dima.babelfish.storage.text.leaf.PointerRope;
import de.tub.dima.babelfish.storage.text.operations.*;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

@ExportLibrary(value = DynamicDispatchLibrary.class )
public abstract class AbstractRope implements Rope {

    @Override
    public boolean equals(Text otherText) {
        if(otherText instanceof StringText){
            if(length() >= otherText.length()){
                for(int i = 0 ; i< otherText.length(); i++){
                    if(get(i)!=otherText.get(i)){
                        return false;
                    }
                }
                return true;
            }
            return false;
        }else{
            if(length() == otherText.length()){
                for(int i = 0 ; i< otherText.length(); i++){
                    if(get(i)!=otherText.get(i)){
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
    }

    @Override
    public Text substring(int start, int end) {
        return new SubstringRope(this, start, end);
    }

    @Override
    public Text concat(Text otherText) {
        if(otherText instanceof StringText){
            StringText string = (StringText) otherText;
            return  new ConcatRope(this, new ConstantRope(string.toString().toCharArray()));
        }else{
            return new ConcatRope(this, (Rope)otherText);
        }
    }

    @Override
    public Text lowercase() {
        return new LowercaseRope(this);
    }

    @Override
    public Text uppercase() {
        return new UppercaseRope(this);
    }

    @Override
    public Text reverse() {
        return new ReserveRope(this, this.length());
    }



    public SplittedRope split(char split) {
        PointerRope r = (PointerRope) this;
        int[] array = new int[128];
        int c = 0;
        for (int i = 0; i < r.length(); i++) {
            if ((char) r.get(i) == split) {
                c++;
                array[c] = i;
            }
        }
        return new SplittedRope((PointerRope) this, split, array, c);
    }

    @ExportMessage
    public Class<?> dispatch() {
        return AbstractRopeBuiltins.class;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i< length();i++){
            sb.append(get(i));
        }
        return sb.toString();
    }
}
