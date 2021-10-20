package de.tub.dima.babelfish.storage.text.leaf;

import com.oracle.truffle.api.CompilerDirectives;
import de.tub.dima.babelfish.storage.text.AbstractRope;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class ConstantRope extends AbstractRope {

    @CompilerDirectives.CompilationFinal(dimensions = 1)
    private final char[] content;

    public ConstantRope(char[] content) {
        this.content = content;
    }

    @Override
    public int length() {
        return content.length;
    }

    @Override
    public char get(int index) {
        return content[index];
    }

    @Override
    public boolean contains(Text otherText) {
        return false;
    }
}
