package de.tub.dima.babelfish.typesytem.udt;

import de.tub.dima.babelfish.storage.UnsafeUtils;

public final class LazyDate extends AbstractDate {

    private final long address;

    public LazyDate(long address) {
        this.address = address;
    }

    @Override
    public int getUnixTs() {
        return UnsafeUtils.getInt(address);
    }

    public boolean before(String other) {
        return this.getUnixTs() < Date.parse(other);
    }

    public boolean after(String other) {
        return this.getUnixTs() > Date.parse(other);
    }
}
