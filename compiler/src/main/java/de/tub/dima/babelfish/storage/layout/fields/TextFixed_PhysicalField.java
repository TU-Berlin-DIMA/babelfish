package de.tub.dima.babelfish.storage.layout.fields;

import com.oracle.truffle.api.CompilerDirectives;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.storage.AddressPointer;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.storage.layout.PhysicalField;
import de.tub.dima.babelfish.storage.text.PointerText;
import de.tub.dima.babelfish.storage.text.leaf.PointerRope;
import de.tub.dima.babelfish.typesytem.variableLengthType.Text;

public class TextFixed_PhysicalField implements PhysicalField<Text> {

    private final String name;
    private final int maxLength;
    @CompilerDirectives.CompilationFinal
    private boolean LAZY_STRING_HANDLING;

    public TextFixed_PhysicalField(String name, int maxLength) {
        this.name = name;
        this.maxLength = maxLength;
        LAZY_STRING_HANDLING = RuntimeConfiguration.LAZY_STRING_HANDLING;
    }

    @Override
    public int getPhysicalSize() {
        return 2 * maxLength;
    }

    @Override
    public LuthStamps getStamp() {
        return LuthStamps.Char;
    }

    @Override
    public Text readValue(AddressPointer addressPointer) {
        //return new NativeFixedText(addressPointer.getAddress(), maxLength);
        if (CompilerDirectives.inInterpreter()) {
            LAZY_STRING_HANDLING = RuntimeConfiguration.LAZY_STRING_HANDLING;
        }
        if (LAZY_STRING_HANDLING) {
            return new PointerRope(addressPointer.getAddress(), maxLength);
        } else {
            return new PointerText(addressPointer.getAddress(), maxLength);
            /*char[] chars = new char[maxLength];
            long address = addressPointer.getAddress();
            int i = 0;
            for (; i < maxLength; i++) {
                char c = UnsafeUtils.getChar(address + (i * 2));
                if(c == '\0'){
                    break;
                }
                chars[i] = c;
            }*/

            // return new StringText(String.valueOf(chars, 0, i));
        }
    }

    @Override
    public void writeValue(AddressPointer bufferAddress, Text value) {

        long address = bufferAddress.getAddress();

        if (value.length() == maxLength) {
            for (int i = 0; i < maxLength; i++) {
                UnsafeUtils.putChar(address + (i * 2), value.get(i));
            }
            return;
        }else{
            System.out.println("huch");
        }

        for (int i = 0; i < maxLength; i++) {
            if (i < value.length()) {
                UnsafeUtils.putChar(address + (i * 2), value.get(i));
            } else {
                UnsafeUtils.putChar(address + (i * 2), '\0');
            }
        }
    }

    @Override
    public String getName() {
        return name;
    }

}
