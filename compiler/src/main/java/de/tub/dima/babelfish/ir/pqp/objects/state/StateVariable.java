package de.tub.dima.babelfish.ir.pqp.objects.state;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.storage.layout.fields.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;
import de.tub.dima.babelfish.typesytem.BFType;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Eager_Float_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_32;
import de.tub.dima.babelfish.typesytem.valueTypes.number.luthfloat.Float_64;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.Numeric;
import de.tub.dima.babelfish.BabelfishEngine;

@ExportLibrary(value = InteropLibrary.class)
public class StateVariable implements TruffleObject {

    protected final StateDescriptor stateDescriptor;

    @CompilerDirectives.CompilationFinal
    protected final long address;


    public StateVariable(StateDescriptor stateDescriptor, long address) {
        this.stateDescriptor = stateDescriptor;
        this.address = address;
    }

    public BFType getDefaultValue() {
        return stateDescriptor.getDefaultValue()[0];
    }

    public void init() {
        UnsafeUtils.putInt(address, 0);
        PhysicalField field = getPhysicalField();
        if (stateDescriptor.getDefaultValue() != null) {
            if (field instanceof Float_32) {
                Fload_32_PhysicalField fload_32_physicalField = (Fload_32_PhysicalField) getPhysicalField();
                Float_32 defaultFloat = (Float_32) stateDescriptor.getDefaultValue()[0];
                fload_32_physicalField.writeValue(new AddressPointer(address), defaultFloat);
            } else if (field instanceof Numeric_PhysicalField) {
                Numeric_PhysicalField numeric_physicalField = (Numeric_PhysicalField) getPhysicalField();
                Numeric defaultNumeric = (Numeric) stateDescriptor.getDefaultValue()[0];
                //if (RuntimeConfiguration.MULTI_THREADED)
                  //  numeric_physicalField.writeValue(new AddressPointer(address + SyncLuthGroupBy.LOCK_OFFSET), defaultNumeric);
                //else
                    numeric_physicalField.writeValue(new AddressPointer(address), defaultNumeric);
            }
        }
    }

    public StateDescriptor getStateDescriptor() {
        return stateDescriptor;
    }

    public long getAddress() {
        return address;
    }

    public Float_32 readFloat() {
        Fload_32_PhysicalField floatField = (Fload_32_PhysicalField) getPhysicalField();
        return (Float_32) floatField.readValue(new AddressPointer(address));
    }

    public Numeric readNumeric() {
        Numeric_PhysicalField numeric_physicalField = (Numeric_PhysicalField) getPhysicalField();
        return (Numeric) numeric_physicalField.readValue(new AddressPointer(address));
    }

    public Float_64 readDouble() {
        Fload_64_PhysicalField floatField = (Fload_64_PhysicalField) getPhysicalField();
        return (Float_64) floatField.readValue(new AddressPointer(address));
    }

    public void writeFloat32(Float_32 value) {
        ((Fload_32_PhysicalField) getPhysicalField()).writeValue(new AddressPointer(address), value);
    }

    public void writeFloat64(Float_64 value) {
        ((Fload_64_PhysicalField) getPhysicalField()).writeValue(new AddressPointer(address), value);
    }

    @ExportMessage
    public static class WriteMember {

        @Specialization
        public static void write(StateVariable var, String member, byte value) {
            write(var, member, new Eager_Int_8(value));
        }

        @Specialization
        public static void write(StateVariable var, String member, short value) {
            write(var, member, new Eager_Int_16(value));
        }

        @Specialization
        public static void write(StateVariable var, String member, int value) {
            ((Int_32_PhysicalField) var.getPhysicalField()).writeValue(new AddressPointer(var.address), new Lazy_Int_32(value));
        }

        @Specialization
        public static void write(StateVariable var, String member, long value) {
            write(var, member, new Eager_Int_64(value));
        }

        @Specialization
        public static void write(StateVariable var, String member, float value) {
            write(var, member, new Eager_Float_32(value));
        }

        @Specialization
        public static void write(StateVariable var, String member, double value) {
            write(var, member, new Eager_Float_64(value));
        }

        @Specialization
        public static void write(StateVariable var, String member, BFType value) {
            var.getPhysicalField().writeValue(new AddressPointer(var.address), value);
        }
    }


    @ExportMessage
    public static class ReadMember {

        public static PhysicalField getPhysicalField(StateDescriptor stateDescriptor) {
            return stateDescriptor.getPhysicalSchema().getField(0);
        }

        public static boolean isNumeric(StateDescriptor stateDescriptor) {
            return getPhysicalField(stateDescriptor) instanceof Numeric_PhysicalField;
        }

        public static Numeric_PhysicalField getNumericField(StateDescriptor stateDescriptor) {
            return (Numeric_PhysicalField) getPhysicalField(stateDescriptor);
        }

        @Specialization(guards = "isNumeric(var.stateDescriptor)")
        public static Object readNumericField(StateVariable var,
                                              String member,
                                              @Cached(value = "getNumericField(var.stateDescriptor)", allowUncached = true) Numeric_PhysicalField physicalField) {
            return physicalField.readValue(new AddressPointer(var.address));
        }

        @Specialization
        public static Object read(StateVariable var, String member) {
            return ((Int_32_PhysicalField) var.getPhysicalField()).readValue(new AddressPointer(var.address)).asInt();
        }
    }

    public PhysicalField getPhysicalField() {
        return stateDescriptor.getPhysicalSchema().getField(0);
    }

    @ExportMessage
    public boolean isMemberReadable(String inclue) {
        return true;
    }

    @ExportMessage
    public Object getMembers(boolean inclue) {
        return true;
    }

    @ExportMessage
    public boolean hasMembers() {
        return true;
    }


    @ExportMessage
    public boolean isMemberInvocable(String inclue) {
        return true;
    }


    @ExportMessage
    public Object invokeMember(String member, Object... arguments) {
        return 10;
    }


    @ExportMessage
    public boolean isMemberModifiable(String member) {
        return true;
    }


    @ExportMessage
    public boolean isMemberInsertable(String member) {
        return true;
    }

    @ExportMessage
    boolean hasLanguage() {
        return false;
    }

    @ExportMessage
    Class<TruffleLanguage<BabelfishEngine.BabelfishContext>> getLanguage() throws UnsupportedMessageException {
        return null;
    }

    @ExportMessage
    Object toDisplayString(boolean allowSideEffects) {
        return null;
    }
}
