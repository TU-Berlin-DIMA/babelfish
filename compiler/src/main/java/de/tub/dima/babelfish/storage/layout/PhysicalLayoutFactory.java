package de.tub.dima.babelfish.storage.layout;

public interface PhysicalLayoutFactory {
    PhysicalLayout create(PhysicalSchema physicalSchema, long targetBufferSize);

    String getName();

    public static class ColumnLayoutFactory implements PhysicalLayoutFactory{
        @Override
        public PhysicalLayout create(PhysicalSchema physicalSchema, long targetBufferSize) {
            System.out.println("Create Column layout");
            return new PhysicalColumnLayout(physicalSchema, targetBufferSize);
        }

        @Override
        public String getName() {
            return "column";
        }
    }

    public static class RowLayoutFactory implements PhysicalLayoutFactory{
        @Override
        public PhysicalLayout create(PhysicalSchema physicalSchema, long targetBufferSize) {
            System.out.println("Create Row layout");
            return new PhysicalRowLayout(physicalSchema);
        }

        @Override
        public String getName() {
            return "row";
        }
    }
}
