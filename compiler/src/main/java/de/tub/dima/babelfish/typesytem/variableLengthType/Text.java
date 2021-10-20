package de.tub.dima.babelfish.typesytem.variableLengthType;

import com.oracle.truffle.api.interop.TruffleObject;

public interface Text extends VariableLengthType, TruffleObject {

    int length();
    char get(int index);
    Text substring(int start, int end);
    boolean equals(Text otherText);
    boolean contains(Text otherText);
    Text concat(Text otherText);
    Text lowercase();
    Text uppercase();
    Text reverse();

    SplittedText split(char split);

}
