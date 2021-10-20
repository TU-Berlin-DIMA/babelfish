package de.tub.dima.babelfish.typesytem.variableLengthType;

import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.InvalidArrayIndexException;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

@ExportLibrary(value = InteropLibrary.class)
public class StringTextArray implements TruffleObject, SplittedText {

    private final StringText[] textArray;

    public StringTextArray(StringText[] textArray) {
        this.textArray = textArray;
    }


    @ExportMessage
    public boolean hasArrayElements() {
        return true;
    }
    @ExportMessage
    public Object readArrayElement(long index) throws UnsupportedMessageException, InvalidArrayIndexException {
        return textArray[(int) index];
    }
    @ExportMessage
    public long getArraySize() {
        return textArray.length;
    }

    @ExportMessage
    public boolean isArrayElementReadable(long index)  {
        return true;
    }


    @Override
    public Text get(int index) {
        return textArray[(int) index];
    }

    @Override
    public int length() {
        return textArray.length;
    }
}
