package de.tub.dima.babelfish.ir.pqp.nodes.state.map.hash;

import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.library.CachedLibrary;
import com.oracle.truffle.api.nodes.ExecutableNode;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import de.tub.dima.babelfish.ir.pqp.nodes.records.ReadLuthFieldNode;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.udt.LazyDate;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.IntLibrary;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.Int_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_64;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

@NodeChild(value = "value", type = ReadLuthFieldNode.class)
public abstract class HashBFValueNode extends ExecutableNode {

    private static final long seed = 902850234L;

    public HashBFValueNode() {
        super(null);
    }

    public static HashBFValueNode create(ReadLuthFieldNode readLuthFieldNode) {
        return HashBFValueNodeGen.create(readLuthFieldNode);
    }

    private long hashKey(long value, long seed) {
        // MurmurHash64A
        long m = 0xc6a4a7935bd1e995L;
        int r = 47;
        long h = seed ^ 0x8445d61a4e774912L ^ (8 * m);
        value *= m;
        value ^= value >> r;
        value *= m;
        h ^= value;
        h *= m;
        h ^= h >> r;
        h *= m;
        h ^= h >> r;
        return h;
    }

    @Specialization
    public long getHash(Int_32 value, @CachedLibrary(limit = "30") IntLibrary lib) {
        return hashKey(lib.asIntValue(value), seed);
    }

    @Specialization
    public long getHash(Int_64 value, @CachedLibrary(limit = "30") IntLibrary lib) {
        return hashKey(lib.asLongValue(value), seed);
    }

    @Specialization
    public long getHash(Float_32 value) {
        return hashKey(Float.floatToRawIntBits(value.asFloat()), seed);
    }

    @Specialization
    public long getHash(Float_64 value) {
        return hashKey(Double.doubleToRawLongBits(value.asDouble()), seed);
    }

    @Specialization
    public long getHash(Char value) {
        return hashKey(value.getChar(), seed);
    }

    @Specialization
    public long getHash(Date value) {
        return hashKey(value.getUnixTs(), seed);
    }

    @Specialization
    public long getHash(LazyDate value) {
        return hashKey(value.getUnixTs(), seed);
    }

    @Specialization
    @ExplodeLoop
    public long getHash(Text value, @Cached(value = "value.length()", allowUncached = true) int cached_value) {
        long m = 0xc6a4a7935bd1e995L;
        int r = 47;

        long h = seed ^ (cached_value * m);

        for (int i = 0; i < cached_value; i++) {
            long k = value.get(i);
            k *= m;
            k ^= k >> r;
            k *= m;
            h ^= k;
            h *= m;
        }
        h ^= h >> r;
        h *= m;
        h ^= h >> r;
        return h;
    }

}
