package de.tub.dima.babelfish.ir.lqp.relational;

import de.tub.dima.babelfish.ir.lqp.schema.FieldReference;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class KeyGroup implements Serializable {

    private List<FieldReference> keys;

    public KeyGroup(FieldReference... fieldReference) {
        keys = Arrays.asList(fieldReference);
    }

    public List<FieldReference> getKeys() {
        return keys;
    }
}
