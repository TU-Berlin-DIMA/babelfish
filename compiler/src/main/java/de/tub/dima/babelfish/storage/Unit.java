package de.tub.dima.babelfish.storage;

import java.util.*;

public class Unit {

    public static class Bytes {
        private final long bytes;

        public Bytes(long bytes) {
            this.bytes = bytes;
        }

        public long getBytes() {
            return bytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bytes bytes1 = (Bytes) o;
            return bytes == bytes1.bytes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(bytes);
        }
    }

    public static class KiloBytes extends Bytes {
        public KiloBytes(long kiloBytes) {
            super(kiloBytes * 1024);
        }
    }

    public static class MegaBytes extends KiloBytes {
        public MegaBytes(long megaBytes) {
            super(megaBytes * 1024);
        }
    }

    public static class GigaBytes extends MegaBytes {
        public GigaBytes(long gigaBytes) {
            super(gigaBytes * 1024);
        }
    }

}
