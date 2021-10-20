package de.tub.dima.babelfish.ir.pqp.nodes.relational.scan.csv;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeField;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;
import de.tub.dima.babelfish.conf.RuntimeConfiguration;
import de.tub.dima.babelfish.ir.pqp.nodes.utils.ArgumentReadNode;
import de.tub.dima.babelfish.storage.UnsafeUtils;
import de.tub.dima.babelfish.storage.text.leaf.CSVSourceRope;
import de.tub.dima.babelfish.typesytem.schema.field.DateField;
import de.tub.dima.babelfish.typesytem.schema.field.NumericField;
import de.tub.dima.babelfish.typesytem.schema.field.SchemaField;
import de.tub.dima.babelfish.typesytem.schema.field.TextField;
import de.tub.dima.babelfish.typesytem.udt.CSVSourceDate;
import de.tub.dima.babelfish.typesytem.udt.Date;
import de.tub.dima.babelfish.typesytem.valueTypes.Char;
import de.tub.dima.babelfish.typesytem.valueTypes.number.integer.*;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.CSVSourceNumeric;
import de.tub.dima.babelfish.typesytem.valueTypes.number.numeric.EagerNumeric;
import de.tub.dima.babelfish.typesytem.variableLengthType.StringText;

public class ParseCSVFieldNodesFactory {

    public static ParseFieldNode createNode(SchemaField field) {
        if (field instanceof NumericField) {
            return ParseNumericField.create(((NumericField) field).getPrecission());
        } else if (field instanceof DateField) {
            return ParseDateField.create();
        } else if (field instanceof TextField) {
            return ParseTextField.create(((TextField) field).getMaxLength());
        } else if (Char.class.isAssignableFrom(field.getType())) {
            return ParseCharField.create();
        } else if (Int_8.class.isAssignableFrom(field.getType())) {
            return ParseInt_8Field.create();
        } else if (Int_16.class.isAssignableFrom(field.getType())) {
            return ParseInt_16Field.create();
        } else if (Int_32.class.isAssignableFrom(field.getType())) {
            return ParseInt_32Field.create();
        } else if (Int_64.class.isAssignableFrom(field.getType())) {
            return ParseInt_64Field.create();
        } else {
            return UndefinedField.create();
        }
    }

    @NodeInfo
    @NodeChild(type = ArgumentReadNode.class, value = "startAddress")
    @NodeChild(type = ArgumentReadNode.class, value = "endAddress")
    public static abstract class ParseFieldNode extends Node {

        protected final boolean LAZY_PARSING = RuntimeConfiguration.LAZY_PARSING;

        public abstract Object execute(VirtualFrame frame);

    }

    @NodeInfo
    @NodeField(name = "precision", type = int.class)
    public static abstract class ParseNumericField extends ParseFieldNode {

        public static ParseNumericField create(int precision) {
            return ParseCSVFieldNodesFactoryFactory
                    .ParseNumericFieldNodeGen.create(
                            new ArgumentReadNode(0),
                            new ArgumentReadNode(1),
                            precision);
        }

        @Specialization(guards = "LAZY_PARSING")
        CSVSourceNumeric parseLazy(long startAddress, long endAddress) {
            return new CSVSourceNumeric(startAddress, endAddress, getPrecision());
        }

        @Specialization()
        EagerNumeric parse(long startAddress, long endAddress) {
            CSVSourceNumeric csvSourceNumeric = new CSVSourceNumeric(startAddress, endAddress, getPrecision());
            return new EagerNumeric(csvSourceNumeric.getValue(), getPrecision());
        }

        abstract int getPrecision();

    }

    @NodeInfo
    public static abstract class ParseDateField extends ParseFieldNode {

        public static ParseDateField create() {
            return ParseCSVFieldNodesFactoryFactory
                    .ParseDateFieldNodeGen.create(
                            new ArgumentReadNode(0),
                            new ArgumentReadNode(1));
        }

        @Specialization(guards = "LAZY_PARSING")
        CSVSourceDate parseLazy(long startAddress, long endAddress) {
            return new CSVSourceDate(startAddress, endAddress);
        }

        @Specialization()
        Date parse(long startAddress, long endAddress) {
            CSVSourceDate csvSource = new CSVSourceDate(startAddress, endAddress);
            return new Date(csvSource.getUnixTs());
        }

    }

    @NodeInfo
    public static abstract class ParseInt_8Field extends ParseFieldNode {

        public static ParseInt_8Field create() {
            return ParseCSVFieldNodesFactoryFactory
                    .ParseInt_8FieldNodeGen.create(
                            new ArgumentReadNode(0),
                            new ArgumentReadNode(1));
        }

        @Specialization(guards = "LAZY_PARSING")
        CSVSourceInt_8 parseLazy(long startAddress, long endAddress) {
            return new CSVSourceInt_8(startAddress, endAddress);
        }

        @Specialization()
        Eager_Int_8 parse(long startAddress, long endAddress) {
            CSVSourceInt_8 csvSource = new CSVSourceInt_8(startAddress, endAddress);
            return new Eager_Int_8(csvSource.asByte());
        }
    }

    @NodeInfo
    public static abstract class ParseInt_16Field extends ParseFieldNode {

        public static ParseInt_16Field create() {
            return ParseCSVFieldNodesFactoryFactory
                    .ParseInt_16FieldNodeGen.create(
                            new ArgumentReadNode(0),
                            new ArgumentReadNode(1));
        }

        @Specialization(guards = "LAZY_PARSING")
        CSVSourceInt_16 parseLazy(long startAddress, long endAddress) {
            return new CSVSourceInt_16(startAddress, endAddress);
        }

        @Specialization()
        Eager_Int_16 parse(long startAddress, long endAddress) {
            CSVSourceInt_16 csvSource = new CSVSourceInt_16(startAddress, endAddress);
            return new Eager_Int_16(csvSource.asShort());
        }
    }


    @NodeInfo
    public static abstract class ParseInt_32Field extends ParseFieldNode {

        public static ParseInt_32Field create() {
            return ParseCSVFieldNodesFactoryFactory
                    .ParseInt_32FieldNodeGen.create(
                            new ArgumentReadNode(0),
                            new ArgumentReadNode(1));
        }

        @Specialization(guards = "LAZY_PARSING")
        CSVSourceInt_32 parseLazy(long startAddress, long endAddress) {
            return new CSVSourceInt_32(startAddress, endAddress);
        }

        @Specialization()
        Eager_Int_32 parse(long startAddress, long endAddress) {
            CSVSourceInt_32 csvSource = new CSVSourceInt_32(startAddress, endAddress);
            return new Eager_Int_32(csvSource.asInt());
        }
    }

    @NodeInfo
    public static abstract class ParseInt_64Field extends ParseFieldNode {

        public static ParseInt_64Field create() {
            return ParseCSVFieldNodesFactoryFactory
                    .ParseInt_64FieldNodeGen.create(
                            new ArgumentReadNode(0),
                            new ArgumentReadNode(1));
        }

        @Specialization(guards = "LAZY_PARSING")
        CSVSourceInt_64 parseLazy(long startAddress, long endAddress) {
            return new CSVSourceInt_64(startAddress, endAddress);
        }

        @Specialization()
        Eager_Int_64 parse(long startAddress, long endAddress) {
            CSVSourceInt_64 csvSource = new CSVSourceInt_64(startAddress, endAddress);
            return new Eager_Int_64(csvSource.asLong());
        }
    }

    @NodeInfo
    @NodeField(name = "size", type = int.class)
    public static abstract class ParseTextField extends ParseFieldNode {

        public static ParseTextField create(long size) {
            return ParseCSVFieldNodesFactoryFactory.ParseTextFieldNodeGen
                    .create(new ArgumentReadNode(0), new ArgumentReadNode(1), (int) size);
        }

        @Specialization(guards = "LAZY_PARSING")
        CSVSourceRope parseLazy(long startAddress, long endAddress) {
            return new CSVSourceRope(startAddress, endAddress, getSize());
        }

        @Specialization()
        StringText parse(long startAddress, long endAddress) {
            CSVSourceRope csvSource = new CSVSourceRope(startAddress, endAddress, getSize());
            return new StringText(csvSource.toString(), getSize());
        }

        abstract int getSize();
    }

    @NodeInfo
    public static abstract class ParseCharField extends ParseFieldNode {

        public static ParseCharField create() {
            return ParseCSVFieldNodesFactoryFactory.ParseCharFieldNodeGen
                    .create(new ArgumentReadNode(0), new ArgumentReadNode(1));
        }

        @Specialization(guards = "LAZY_PARSING")
        Char parseLazy(long startAddress, long endAddress) {
            //System.out.println("parse null field: " + startAddress + " - " + endAddress);
            byte charValue = UnsafeUtils.getByte(startAddress);
            return new Char((char) charValue);
        }

        @Specialization()
        Char parse(long startAddress, long endAddress) {
            //System.out.println("parse null field: " + startAddress + " - " + endAddress);
            byte charValue = UnsafeUtils.getByte(startAddress);
            return new Char((char) charValue);
        }

    }


    @NodeInfo
    public static abstract class UndefinedField extends ParseFieldNode {

        public static UndefinedField create() {
            return ParseCSVFieldNodesFactoryFactory.UndefinedFieldNodeGen
                    .create(new ArgumentReadNode(0), new ArgumentReadNode(1));
        }

        @Specialization(guards = "LAZY_PARSING")
        EagerNumeric parse(long startAddress, long endAddress) {
            //System.out.println("parse null field: " + startAddress + " - " + endAddress);

            return null;
        }

    }

}
