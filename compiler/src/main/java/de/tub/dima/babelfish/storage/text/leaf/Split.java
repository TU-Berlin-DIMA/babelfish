package de.tub.dima.babelfish.storage.text.leaf;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import de.tub.dima.babelfish.storage.text.operations.SplittedRope;

public class Split {

    @CompilerDirectives.CompilationFinal
    private static int length;

    @ExplodeLoop
    public static SplittedRope split(PointerRope r, char split) {
        int[] array = new int[128];
        int c = 0;
        if(CompilerDirectives.inInterpreter()){
            length = r.length();
        }

        for (int i = 0; i < length; i++) {
            if ((char) r.get(i) == split) {
                c++;
                array[c] = i;
            }
        }
        return new SplittedRope(r, split, array, c);
    }

}
