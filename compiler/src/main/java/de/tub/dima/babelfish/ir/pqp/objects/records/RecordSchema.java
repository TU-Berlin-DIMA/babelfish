package de.tub.dima.babelfish.ir.pqp.objects.records;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.nodes.ExplodeLoop;

/**
 * The record schema describes the fields of a BFRecord.
 * It is assumed to be compile-time constant.
 */
public class RecordSchema {

    @CompilerDirectives.CompilationFinal(dimensions = 1)
    public final String[] fieldNames;

    @CompilerDirectives.CompilationFinal
    public int last = 0;


    public RecordSchema() {
        this(new String[32]);
    }

    private RecordSchema(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public int getSize(){
        return last;
    }

    public RecordSchema addField(String name) {
        if (!containsField(name)) {
           // String[] newFields = Arrays.copyOf(fieldNames, fieldNames.length + 1);
            //newFields[newFields.length - 1] = name;
            fieldNames[last++] = name;
            return this;
        }
        return this;
    }

    public boolean containsField(String name){
        return  getFieldIndex(name) != -1;
    }

    @ExplodeLoop
    @CompilerDirectives.TruffleBoundary
    public int getFieldIndex(String name) {
        for (int i = 0; i <  last; i++) {
            if (fieldNames[i]!= null && fieldNames[i].equals(name))
                return i;
        }
        return -1;
    }

    @ExplodeLoop
    public int getFieldIndexFromConstant(String name) {
        return getFieldIndexDyn(name);
    }

    public int getFieldIndexDyn(String name) {
        for (int i = 0; i <  last; i++) {
            if (fieldNames[i]!= null && fieldNames[i].equals(name))
                return i;
        }
        return -1;
    }

    public RecordSchema copy(){
        RecordSchema schema = new RecordSchema();
        for(int i = 0; i <  last; i++){
            schema.addField(fieldNames[i]);
        }
        return schema;
    }


}
